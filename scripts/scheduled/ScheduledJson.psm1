#requires -Version 7
# Preserves JSON strings at the scheduled GitHub, native-tool and local persistence boundaries.
# These readers also run before Rust preparation. System.Text.Json is part of PowerShell 7's
# runtime; it avoids the date coercion of ConvertFrom-Json without requiring PowerShell 7.5's
# DateKind parameter. Domain validation and canonical digest verification remain with callers.
# Ref: ../../.github/workflows/implementation.md#lossless-evidence-transport.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function ConvertFrom-ScheduledJson {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory, ValueFromPipeline)][AllowEmptyString()][string] $InputObject,
        [switch] $NoEnumerate
    )
    process {
        # Match the existing reader's nesting limit; callers retain their stricter wire limits.
        $options = [System.Text.Json.JsonDocumentOptions]::new()
        $options.MaxDepth = 1024
        try {
            $document = [System.Text.Json.JsonDocument]::Parse($InputObject, $options)
        } catch [System.Text.Json.JsonException] {
            throw [ArgumentException]::new('Invalid scheduled JSON.', $_.Exception)
        }
        try {
            $value = ConvertFrom-ScheduledJsonElement $document.RootElement
            if ($NoEnumerate) { return ,$value }
            return $value
        } finally { $document.Dispose() }
    }
}

function ConvertFrom-ScheduledJsonElement {
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseLiteralInitializerForHashtable', '',
        Justification = 'JSON member names are case-sensitive; a literal hashtable can merge distinct keys.')]
    [CmdletBinding()]
    param([System.Text.Json.JsonElement] $Element)
    switch ($Element.ValueKind) {
        Object {
            $value = [hashtable]::new([StringComparer]::Ordinal)
            foreach ($property in $Element.EnumerateObject()) {
                $value[$property.Name] = ConvertFrom-ScheduledJsonElement $property.Value
            }
            return $value
        }
        Array {
            $value = [Collections.Generic.List[object]]::new()
            foreach ($item in $Element.EnumerateArray()) {
                $value.Add((ConvertFrom-ScheduledJsonElement $item))
            }
            return ,$value.ToArray()
        }
        String { return $Element.GetString() }
        # Retain PowerShell's existing numeric representation, but never pass strings to its
        # date-aware reader. In particular, run/job identifiers must remain integer values.
        Number { return ConvertFrom-Json -InputObject $Element.GetRawText() }
        True { return $true }
        False { return $false }
        Null { return $null }
        default { throw [ArgumentException]::new('Unsupported scheduled JSON value.') }
    }
}

Export-ModuleMember -Function ConvertFrom-ScheduledJson
