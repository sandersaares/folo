set windows-shell := ["pwsh.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command"]
set shell := ["pwsh", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command"]
set script-interpreter := ["pwsh", "-NoLogo", "-NoProfile", "-NonInteractive"]

# Constants shared by Just commands and GitHub workflows.
set dotenv-path := "./constants.env"
set dotenv-required := true

package := ""
target_package := if package == "" { " --workspace" } else { " -p " + replace(package, " ", " -p ") }

_default:
    @just --list

import 'justfiles/just_basics.just'
import 'justfiles/just_automation.just'
import 'justfiles/just_book.just'
import 'justfiles/just_delta.just'
import 'justfiles/just_quality.just'
import 'justfiles/just_release.just'
import 'justfiles/just_setup.just'
import 'justfiles/just_scheduled.just'
import 'justfiles/just_testing.just'
