# GitHub's --auto can merge immediately when no enforced check blocks the caller.
# Require a repository ruleset with no bypass actors, not merely a named check in CI.
function Assert-AutoMergeProtection {
    param(
        [Parameter(Mandatory)][string]$Repo,
        [Parameter(Mandatory)][string]$Branch,
        [scriptblock]$ReadApi = {
            param($endpoint)
            $raw = gh api $endpoint --paginate --slurp 2>$null
            if ($LASTEXITCODE -ne 0) { throw "could not read $endpoint (exit $LASTEXITCODE)" }
            $pages = ConvertFrom-Json -InputObject ($raw -join "`n") -NoEnumerate
            if ($pages -isnot [array]) { throw "invalid API response from $endpoint" }
            foreach ($page in $pages) { $page }
        }
    )

    $branchPath = [Uri]::EscapeDataString($Branch)
    $rules = @(& $ReadApi "repos/$Repo/rules/branches/$branchPath")
    foreach ($rule in $rules) {
        if ($rule.type -ne 'required_status_checks' -or $rule.ruleset_source_type -ne 'Repository' -or
            $rule.ruleset_source -ne $Repo -or $rule.ruleset_id -le 0) { continue }

        $required = @($rule.parameters.required_status_checks | Where-Object {
            $_.context -ceq 'CI Passed' -and $_.integration_id -eq 15368 # GitHub Actions
        })
        if ($required.Count -eq 0) { continue }

        $responses = @(& $ReadApi "repos/$Repo/rulesets/$($rule.ruleset_id)")
        if ($responses.Count -ne 1) { throw 'could not read the enforcing ruleset' }
        $ruleset = $responses[0]
        if ($ruleset.id -ne $rule.ruleset_id -or $ruleset.source -ne $Repo -or
            $ruleset.target -ne 'branch' -or $ruleset.enforcement -ne 'active' -or
            $ruleset.current_user_can_bypass -ne 'never' -or $ruleset.bypass_actors -isnot [array] -or
            $ruleset.bypass_actors.Count -ne 0) { continue }

        # Re-check the detail response too: configuration can change between API reads.
        foreach ($detail in $ruleset.rules) {
            if ($detail.type -ne 'required_status_checks') { continue }
            foreach ($check in $detail.parameters.required_status_checks) {
                if ($check.context -ceq 'CI Passed' -and $check.integration_id -eq 15368) { return }
            }
        }
    }

    throw "no active repository ruleset on $Repo/$Branch requires CI Passed from GitHub Actions with no bypass actors; restore protection before arming"
}
