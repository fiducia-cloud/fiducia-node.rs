#!/usr/bin/env bash
set -euo pipefail

export CI="${CI:-1}"
export NO_COLOR="${NO_COLOR:-1}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
rust_test_threads="${RUST_TEST_THREADS:-4}"

if [ "$(uname -s)" = "Darwin" ]; then
	# Rust's panic unwinder must be linked by the platform driver on macOS.
	# The Nix clang wrapper can produce a binary whose expected-panic tests
	# abort in __rust_start_panic instead of unwinding.
	export CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER="${CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER:-/usr/bin/clang}"
	export CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER="${CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER:-/usr/bin/clang}"
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

cache_root="${NIX_AGENT_CACHE_ROOT:-$repo_root/.cache/nix-agent}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$cache_root/xdg}"
export RUSTUP_HOME="${RUSTUP_HOME:-$cache_root/rustup}"
export CARGO_HOME="${CARGO_HOME:-$cache_root/cargo}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$cache_root/cargo-target}"
mkdir -p "$XDG_CACHE_HOME" "$RUSTUP_HOME" "$CARGO_HOME" "$CARGO_TARGET_DIR"

routing_ref="c694bc5c58587bec12989a347e926c0040aacada"
interfaces_ref="bd718cd72d72aa330534f3688f8fb1ce90c19d10"
read -r flags_mode flags_ref flags_stage flags_path < <(git ls-files --stage vendor/flags-2-env)
if [ "$flags_mode" != "160000" ] || [ "$flags_stage" != "0" ] || [ "$flags_path" != "vendor/flags-2-env" ]; then
	printf 'vendor/flags-2-env is not a committed stage-0 gitlink\n' >&2
	exit 1
fi
workspace_root="$cache_root/workspaces/fiducia-node-${routing_ref:0:12}-${interfaces_ref:0:12}-${flags_ref:0:12}"
node_checkout="$workspace_root/fiducia-node.rs"
flags_binary="$node_checkout/vendor/flags-2-env/build/flags2env"

run_preflight() {
	git diff --check
	nixfmt --check flake.nix .nix/flake.nix .nix/dev-shell.nix
	shellcheck .nix/agent-check.sh
	shfmt -d .nix/agent-check.sh
	actionlint \
		.github/workflows/ci.yml \
		.github/workflows/cli-flags.yml \
		.github/workflows/docker.yml \
		.github/workflows/formal-methods.yml \
		.github/workflows/nix.yml
	python3 scripts/check-pure-read-paths.py
	nix flake check --show-trace
	nix flake check ./.nix --show-trace
}

run_rust_toolchain() {
	rustup set profile minimal
	rustup toolchain install 1.95.0 --profile minimal --component rustfmt --component clippy
	export RUSTUP_TOOLCHAIN=1.95.0
	rustc --version
	cargo --version
}

activate_rust_toolchain() {
	export RUSTUP_TOOLCHAIN=1.95.0
}

ensure_checkout() {
	local destination="$1"
	local url="$2"
	local reference="$3"
	local actual

	mkdir -p "$destination"
	if [ ! -d "$destination/.git" ]; then
		git -C "$destination" init --quiet
		git -C "$destination" remote add origin "$url"
	else
		git -C "$destination" remote set-url origin "$url"
	fi

	git -C "$destination" fetch --depth 1 origin "$reference"
	git -C "$destination" switch --detach --force FETCH_HEAD
	actual="$(git -C "$destination" rev-parse HEAD)"
	if [ "$actual" != "$reference" ]; then
		printf 'expected %s at %s, found %s\n' "$reference" "$destination" "$actual" >&2
		return 1
	fi
}

prepare_workspace() {
	mkdir -p "$node_checkout"
	rsync \
		--archive \
		--delete \
		--exclude '/.git/' \
		--exclude '/.cache/' \
		--exclude '/.formal-artifacts/' \
		--exclude '/_apalache-out/' \
		--exclude '/target/' \
		"$repo_root/" \
		"$node_checkout/"

	ensure_checkout \
		"$workspace_root/fiducia-routing.rs" \
		"https://github.com/fiducia-cloud/fiducia-routing.rs.git" \
		"$routing_ref"
	ensure_checkout \
		"$workspace_root/fiducia-interfaces" \
		"https://github.com/fiducia-cloud/fiducia-interfaces.git" \
		"$interfaces_ref"
	ensure_checkout \
		"$node_checkout/vendor/flags-2-env" \
		"https://github.com/ORESoftware/flags-2-env.git" \
		"$flags_ref"
}

run_flags_build() {
	prepare_workspace
	make -B -C "$node_checkout/vendor/flags-2-env" all
	test -x "$flags_binary"
}

run_flags_audit() {
	if [ ! -x "$flags_binary" ]; then
		run_flags_build
	fi
	cd "$node_checkout"
	"$flags_binary" audit .cli-flags.toml
}

run_flags() {
	run_flags_build
	run_flags_audit
}

run_fmt() {
	activate_rust_toolchain
	prepare_workspace
	cd "$node_checkout"
	cargo fmt --all -- --check
}

run_clippy() {
	activate_rust_toolchain
	prepare_workspace
	cd "$node_checkout"
	cargo clippy --all-targets --all-features --locked -- -D warnings
}

run_tests() {
	activate_rust_toolchain
	prepare_workspace
	cd "$node_checkout"
	cargo test --all-targets --all-features --locked -- \
		--test-threads="$rust_test_threads"
}

run_audit() {
	activate_rust_toolchain
	prepare_workspace
	cd "$node_checkout"
	cargo audit
}

run_formal_typecheck() {
	cd "$repo_root"
	mkdir -p .formal-artifacts/typecheck
	quint typecheck formal/union_lock.qnt 2>&1 |
		tee .formal-artifacts/typecheck/union-lock.log
	quint typecheck formal/union_lock_test.qnt 2>&1 |
		tee .formal-artifacts/typecheck/union-lock-test.log
}

run_formal_test() {
	cd "$repo_root"
	mkdir -p .formal-artifacts/tests
	quint test \
		formal/union_lock_test.qnt \
		--main=union_lock_test \
		--match='.*Test$' \
		--out-itf='.formal-artifacts/tests/{test}-{seq}.itf.json' 2>&1 |
		tee .formal-artifacts/tests/quint-test.log
}

run_formal_simulate() {
	cd "$repo_root"
	mkdir -p .formal-artifacts/simulation
	quint run \
		formal/union_lock.qnt \
		--main=union_lock \
		--max-samples=10000 \
		--max-steps=35 \
		--seed=56620260730 \
		--invariant=union_lock_safety \
		--witnesses \
		queued_work_reached \
		concurrent_disjoint_grants_reached \
		cancellation_tombstone_reached \
		token_exhaustion_reached 2>&1 |
		tee .formal-artifacts/simulation/quint-run.log
}

run_formal_mbt() {
	cd "$repo_root"
	mkdir -p .formal-artifacts/mbt
	quint run \
		formal/union_lock.qnt \
		--main=union_lock \
		--max-samples=500 \
		--max-steps=25 \
		--n-traces=8 \
		--seed=56620260729 \
		--mbt \
		--out-itf='.formal-artifacts/mbt/union-lock-{seq}.itf.json' 2>&1 |
		tee .formal-artifacts/mbt/quint-run.log
}

run_formal_verify_profile() {
	local profile="$1"
	local depth="$2"

	cd "$repo_root"
	mkdir -p ".formal-artifacts/verify-$profile"
	quint verify \
		formal/union_lock.qnt \
		--main=union_lock \
		--max-steps="$depth" \
		--invariant=union_lock_safety \
		--out-itf=".formal-artifacts/verify-$profile/counterexample-{seq}.itf.json" \
		--verbosity=1 2>&1 |
		tee ".formal-artifacts/verify-$profile/quint-verify.log"
}

run_formal_verify() {
	run_formal_verify_profile fast 5
}

run_formal_verify_deep() {
	run_formal_verify_profile deep 6
}

run_formal_refinement() {
	local traces=("$repo_root"/.formal-artifacts/mbt/*.itf.json)
	if [ ! -e "${traces[0]}" ]; then
		run_formal_mbt
	fi
	run_rust_toolchain
	prepare_workspace
	cd "$node_checkout"
	mkdir -p "$repo_root/.formal-artifacts/rust"
	FIDUCIA_ITF_TRACE_DIR="$repo_root/.formal-artifacts/mbt" \
		FIDUCIA_REQUIRE_ITF_REPLAY=1 \
		cargo test --test formal_union_lock_refinement --locked -- \
		--nocapture --test-threads="$rust_test_threads" 2>&1 |
		tee "$repo_root/.formal-artifacts/rust/refinement.log"
}

run_formal_provenance() {
	cd "$repo_root"
	mkdir -p .formal-artifacts
	{
		printf 'commit=%s\n' "${GITHUB_SHA:-$(git rev-parse HEAD)}"
		printf 'event=%s\n' "${GITHUB_EVENT_NAME:-local}"
		printf 'runner_os=%s\n' "${RUNNER_OS:-$(uname -s)}"
		printf 'node='
		node --version
		printf 'java='
		java -version 2>&1 | head -n 1
		printf 'quint='
		quint --version
		printf 'rustc='
		activate_rust_toolchain
		rustc --version
		printf 'cargo='
		cargo --version
		sha256sum \
			flake.lock \
			formal/fm.toml \
			formal/union_lock.qnt \
			formal/union_lock_test.qnt \
			tests/formal_union_lock_refinement.rs
	} >.formal-artifacts/provenance.txt
}

run_formal() {
	run_formal_typecheck
	run_formal_test
	run_formal_simulate
	run_formal_mbt
	run_formal_verify
	run_formal_refinement
	run_formal_provenance
}

run_workspace() {
	activate_rust_toolchain
	prepare_workspace
	cd "$node_checkout"
	make -B -C vendor/flags-2-env all
	vendor/flags-2-env/build/flags2env audit .cli-flags.toml
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features --locked -- -D warnings
	cargo test --all-targets --all-features --locked -- \
		--test-threads="$rust_test_threads"
	cargo audit
}

case "${1:-all}" in
preflight)
	run_preflight
	;;
rust)
	run_rust_toolchain
	;;
bootstrap)
	prepare_workspace
	;;
flags-build)
	run_flags_build
	;;
flags-audit)
	run_flags_audit
	;;
flags)
	run_flags
	;;
fmt)
	run_fmt
	;;
clippy)
	run_clippy
	;;
test)
	run_tests
	;;
audit)
	run_audit
	;;
formal-typecheck)
	run_formal_typecheck
	;;
formal-test)
	run_formal_test
	;;
formal-simulate)
	run_formal_simulate
	;;
formal-mbt)
	run_formal_mbt
	;;
formal-verify)
	run_formal_verify
	;;
formal-verify-deep)
	run_formal_verify_deep
	;;
formal-refinement)
	run_formal_refinement
	;;
formal-provenance)
	run_formal_provenance
	;;
formal)
	run_formal
	;;
workspace)
	run_workspace
	;;
all)
	run_preflight
	run_rust_toolchain
	run_workspace
	;;
*)
	printf '%s\n' \
		'usage: agent-check <command>' \
		'commands: all preflight rust bootstrap flags-build flags-audit flags' \
		'          fmt clippy test audit workspace formal formal-typecheck' \
		'          formal-test formal-simulate formal-mbt formal-verify' \
		'          formal-verify-deep formal-refinement formal-provenance' >&2
	exit 64
	;;
esac
