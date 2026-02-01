# Financial KPI Extraction Pipeline - Master Orchestrator
# Usage: .\run_pipeline.ps1 -PDFPath "input\annual_reports\TCS_AR_2024.pdf"

param(
    [Parameter(Mandatory=$true)]
    [string]$PDFPath,
    
    [string]$OutputBase = "output",
    
    [switch]$UseGemini,
    [switch]$SkipStage1,
    [switch]$SkipStage5,
    [switch]$SkipStage6
    # Removed $Verbose because PowerShell includes it automatically
)

$ErrorActionPreference = "Stop"

# Color output functions
function Write-Stage {
    param([string]$Message)
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host $Message -ForegroundColor Cyan
    Write-Host "========================================`n" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "OK: $Message" -ForegroundColor Green
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "FAIL: $Message" -ForegroundColor Red
}

function Write-Info {
    param([string]$Message)
    Write-Host "  $Message" -ForegroundColor Gray
}

function Write-Warning-Custom {
    param([string]$Message)
    Write-Host "WARN: $Message" -ForegroundColor Yellow
}

function Check-Mode {
    $settingsPath = "config\settings.json"
    
    if (-not (Test-Path $settingsPath)) {
        Write-Error-Custom "Settings file not found: $settingsPath"
        Write-Info "Run: python scripts\verify_setup.py"
        exit 1
    }
    
    try {
        $settings = Get-Content $settingsPath | ConvertFrom-Json
        $mode = $settings.mode
        
        Write-Info "Running in mode: $mode"
        
        if ($mode -eq "pilot") {
            Write-Info "Using on-demand instances (immediate / higher cost)"
        } else {
            Write-Info "Using spot instances (wait for cheap GPUs)"
        }
    } catch {
        Write-Warning-Custom "Could not read mode from settings. Defaulting to pilot."
    }
}

# Verify PDF exists
if (-not (Test-Path $PDFPath)) {
    Write-Error-Custom "PDF file not found: $PDFPath"
    exit 1
}

$PDFPath = (Resolve-Path $PDFPath).Path
$PDFName = Split-Path $PDFPath -Leaf
$DocumentID = [System.IO.Path]::GetFileNameWithoutExtension($PDFName)

Write-Host "`n------------------------------------------------------------" -ForegroundColor Yellow
Write-Host "  FINANCIAL KPI EXTRACTION PIPELINE" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Yellow
Write-Info "Document: $PDFName"
Write-Info "Document ID: $DocumentID"
Write-Info "Start time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"

Check-Mode

if ($UseGemini) {
    Write-Warning-Custom "Using Gemini API instead of Vast.ai"
    Write-Info "This is for testing only. Production should use Vast.ai."
}

Write-Host ""

$TotalStartTime = Get-Date
$TotalCost = 0.0

# Stage 0: Structure Extraction
Write-Stage "STAGE 0: DOCUMENT STRUCTURE EXTRACTION"
$Stage0Start = Get-Date

try {
    python scripts/stage0_structure.py "$PDFPath" --output-dir "$OutputBase/stage0_structure"
    
    if ($LASTEXITCODE -ne 0) {
        throw "Stage 0 failed with exit code $LASTEXITCODE"
    }
    
    $Stage0Duration = (Get-Date) - $Stage0Start
    Write-Success "Stage 0 completed in $($Stage0Duration.TotalSeconds.ToString('F1'))s"
    
    $StructureJSON = "$OutputBase/stage0_structure/${DocumentID}_structure.json"
    
} catch {
    Write-Error-Custom "Stage 0 failed: $_"
    exit 1
}

# Stage 1: Page Selection
if (-not $SkipStage1) {
    Write-Stage "STAGE 1: PAGE SELECTION"
    $Stage1Start = Get-Date
    
    try {
        if ($UseGemini) {
            Write-Info "Using Gemini API fallback..."
            python scripts/stage1_page_selection_gemini.py "$StructureJSON" "$PDFPath" --output-dir "$OutputBase/stage1_flagged_pages"
            $Stage1Cost = 0.01
        } else {
            Write-Info "Using Vast.ai (Llama 3.2 3B)..."
            python scripts/stage1_page_selection_vast.py "$StructureJSON" "$PDFPath" --output-dir "$OutputBase/stage1_flagged_pages"
            $Stage1Cost = 0.013
        }
        
        if ($LASTEXITCODE -ne 0) {
            throw "Stage 1 failed with exit code $LASTEXITCODE"
        }
        
        $Stage1Duration = (Get-Date) - $Stage1Start
        $TotalCost += $Stage1Cost
        
        Write-Success "Stage 1 completed in $($Stage1Duration.TotalMinutes.ToString('F1')) minutes"
        Write-Info "Estimated cost: `$$($Stage1Cost.ToString('F3'))"
        
        $FlaggedJSON = "$OutputBase/stage1_flagged_pages/${DocumentID}_flagged.json"
        
    } catch {
        Write-Error-Custom "Stage 1 failed: $_"
        exit 1
    }
} else {
    Write-Info "Stage 1 skipped (using all candidate pages)"
    Write-Warning-Custom "Skip Stage 1 not fully implemented - exiting"
    exit 1
}

# Stage 2: Image Conversion
Write-Stage "STAGE 2: IMAGE CONVERSION"
$Stage2Start = Get-Date

try {
    python scripts/stage2_convert_images.py "$FlaggedJSON" "$PDFPath" --output-dir "$OutputBase/stage2_images" --dpi 300
    
    if ($LASTEXITCODE -ne 0) {
        throw "Stage 2 failed with exit code $LASTEXITCODE"
    }
    
    $Stage2Duration = (Get-Date) - $Stage2Start
    Write-Success "Stage 2 completed in $($Stage2Duration.TotalSeconds.ToString('F1'))s"
    
    $ManifestJSON = "$OutputBase/stage2_images/$DocumentID/manifest.json"
    
} catch {
    Write-Error-Custom "Stage 2 failed: $_"
    exit 1
}

# Stage 3: KPI Extraction
Write-Stage "STAGE 3: KPI EXTRACTION"
$Stage3Start = Get-Date

try {
    if ($UseGemini) {
        Write-Warning-Custom "Using Gemini Vision API..."
        Write-Info "This will be expensive (~`$2-3 per document)"
        python scripts/stage3_extract_kpis_gemini.py "$ManifestJSON" "$StructureJSON" --output-dir "$OutputBase/stage3_extractions"
        $Stage3Cost = 2.50
    } else {
        Write-Info "Using Vast.ai (Qwen2.5-VL-72B)..."
        Write-Info "This may take 10-15 minutes (model download + inference)"
        python scripts/stage3_extract_kpis_vast.py "$ManifestJSON" "$StructureJSON" --output-dir "$OutputBase/stage3_extractions"
        $Stage3Cost = 0.34
    }
    
    if ($LASTEXITCODE -ne 0) {
        throw "Stage 3 failed with exit code $LASTEXITCODE"
    }
    
    $Stage3Duration = (Get-Date) - $Stage3Start
    $TotalCost += $Stage3Cost
    
    Write-Success "Stage 3 completed in $($Stage3Duration.TotalMinutes.ToString('F1')) minutes"
    Write-Info "Estimated cost: `$$($Stage3Cost.ToString('F2'))"
    
    $ExtractionsDir = "$OutputBase/stage3_extractions/$DocumentID"
    
} catch {
    Write-Error-Custom "Stage 3 failed: $_"
    Write-Info "Check logs/ for detailed error information"
    exit 1
}

# Stage 4: Consolidation
Write-Stage "STAGE 4: CONSOLIDATION"
$Stage4Start = Get-Date

try {
    python scripts/stage4_consolidate.py "$ExtractionsDir" --output-dir "$OutputBase/stage4_consolidated"
    
    if ($LASTEXITCODE -ne 0) {
        throw "Stage 4 failed with exit code $LASTEXITCODE"
    }
    
    $Stage4Duration = (Get-Date) - $Stage4Start
    Write-Success "Stage 4 completed in $($Stage4Duration.TotalSeconds.ToString('F1'))s"
    
    $ConsolidatedJSON = "$OutputBase/stage4_consolidated/${DocumentID}_consolidated.json"
    
} catch {
    Write-Error-Custom "Stage 4 failed: $_"
    exit 1
}

# Stage 4.5: Garbage Filter
Write-Stage "STAGE 4.5: GARBAGE FILTER"
$Stage45Start = Get-Date

try {
    python scripts/stage4_5_filter_garbage.py "$ConsolidatedJSON" --output-dir "$OutputBase/stage4_5_filtered"
    
    if ($LASTEXITCODE -ne 0) {
        throw "Stage 4.5 failed with exit code $LASTEXITCODE"
    }
    
    $Stage45Duration = (Get-Date) - $Stage45Start
    Write-Success "Stage 4.5 completed in $($Stage45Duration.TotalSeconds.ToString('F1'))s"
    
    $FilteredJSON = "$OutputBase/stage4_5_filtered/${DocumentID}_filtered.json"
    
} catch {
    Write-Error-Custom "Stage 4.5 failed: $_"
    exit 1
}

# Stage 5: Self-Verification
if (-not $SkipStage5) {
    Write-Stage "STAGE 5: SELF-VERIFICATION"
    $Stage5Start = Get-Date
    
    try {
        if ($UseGemini) {
            Write-Info "Using Gemini API fallback..."
            python scripts/stage5_self_verify_gemini.py "$FilteredJSON" "$ManifestJSON" --output-dir "$OutputBase/stage5_verified"
            $Stage5Cost = 1.00
        } else {
            Write-Info "Using Vast.ai (Qwen2.5-VL-72B)..."
            python scripts/stage5_self_verify_vast.py "$FilteredJSON" "$ManifestJSON" --output-dir "$OutputBase/stage5_verified"
            $Stage5Cost = 0.12
        }
        
        if ($LASTEXITCODE -ne 0) {
            throw "Stage 5 failed with exit code $LASTEXITCODE"
        }
        
        $Stage5Duration = (Get-Date) - $Stage5Start
        $TotalCost += $Stage5Cost
        
        Write-Success "Stage 5 completed in $($Stage5Duration.TotalMinutes.ToString('F1')) minutes"
        Write-Info "Estimated cost: `$$($Stage5Cost.ToString('F2'))"
        
        $VerifiedJSON = "$OutputBase/stage5_verified/${DocumentID}_verified.json"
        
    } catch {
        Write-Error-Custom "Stage 5 failed: $_"
        exit 1
    }
} else {
    Write-Info "Stage 5 skipped"
    $VerifiedJSON = $FilteredJSON
}

# Stage 6: Gemini Review
if (-not $SkipStage6) {
    Write-Stage "STAGE 6: GEMINI REVIEW (Low Confidence Items Only)"
    $Stage6Start = Get-Date
    
    try {
        python scripts/stage6_gemini_review.py "$VerifiedJSON" "$ManifestJSON" --output-dir "$OutputBase/stage6_gemini_reviewed" --threshold 0.70
        
        if ($LASTEXITCODE -ne 0) {
            throw "Stage 6 failed with exit code $LASTEXITCODE"
        }
        
        $Stage6Duration = (Get-Date) - $Stage6Start
        $Stage6Cost = 0.15
        $TotalCost += $Stage6Cost
        
        Write-Success "Stage 6 completed in $($Stage6Duration.TotalMinutes.ToString('F1')) minutes"
        Write-Info "Estimated cost: `$$($Stage6Cost.ToString('F2'))"
        
        $FinalJSON = "$OutputBase/stage6_gemini_reviewed/${DocumentID}_gemini_reviewed.json"
        
    } catch {
        Write-Error-Custom "Stage 6 failed: $_"
        exit 1
    }
} else {
    Write-Info "Stage 6 skipped"
    $FinalJSON = $VerifiedJSON
}

# Stage 7: CSV Export
Write-Stage "STAGE 7: CSV EXPORT"
$Stage7Start = Get-Date

try {
    python scripts/stage7_export_csv.py "$FinalJSON" --output-dir "$OutputBase/final"
    
    if ($LASTEXITCODE -ne 0) {
        throw "Stage 7 failed with exit code $LASTEXITCODE"
    }
    
    $Stage7Duration = (Get-Date) - $Stage7Start
    Write-Success "Stage 7 completed in $($Stage7Duration.TotalSeconds.ToString('F1'))s"
    
    $OutputCSV = "$OutputBase/final/${DocumentID}_extractions.csv"
    $MasterCSV = "$OutputBase/final/extractions_for_bq.csv"
    
} catch {
    Write-Error-Custom "Stage 7 failed: $_"
    exit 1
}

# Final Summary
$TotalDuration = (Get-Date) - $TotalStartTime

Write-Host "`n------------------------------------------------------------" -ForegroundColor Green
Write-Host "  PIPELINE COMPLETED SUCCESSFULLY" -ForegroundColor Green
Write-Host "------------------------------------------------------------" -ForegroundColor Green

Write-Host "`nResults:" -ForegroundColor Yellow
Write-Info "Document CSV: $OutputCSV"
Write-Info "Master CSV: $MasterCSV"

Write-Host "`nPerformance:" -ForegroundColor Yellow
Write-Info "Total time: $($TotalDuration.TotalMinutes.ToString('F1')) minutes"
Write-Info "Estimated cost: `$$($TotalCost.ToString('F2'))"

if ($UseGemini) {
    Write-Warning-Custom "Used Gemini API - production should use Vast.ai for lower costs"
}

Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Info "1. Review the CSV: $OutputCSV"
Write-Info "2. Check quality metrics in logs/"
Write-Info "3. Review any items flagged for manual review"

Write-Host ""