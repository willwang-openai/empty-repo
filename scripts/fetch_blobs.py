#!/usr/bin/env python3
"""Fetch blobs for a specific commit using dulwich.

This script performs a partial fetch from a remote repository to retrieve only
objects reachable from a specific commit. It first fetches the commit and its
trees without blobs using Git's object filtering, then fetches the blobs that
were omitted in the first step.
"""
import argparse
import os
import tempfile
from dulwich.client import (
    get_transport_and_path,
    SubprocessGitClient,
    HttpGitClient,
)
from dulwich.repo import Repo


def fetch_commit(repo_url: str, commit_sha: str, repo_path: str) -> Repo:
    """Fetch commit and tree objects without blobs.

    If fetching via the default HTTP client fails, retry with a subprocess
    client that shells out to the local ``git`` binary. This works around
    potential issues with HTTP transport implementations.
    """

    os.makedirs(repo_path, exist_ok=True)
    repo = Repo.init_bare(repo_path)
    client, path = get_transport_and_path(repo_url)

    def wants(refs, depth=None, **kwargs):
        return [bytes.fromhex(commit_sha)]

    try:
        client.fetch(
            path,
            repo,
            determine_wants=wants,
            filter_spec=b"blob:none",
            depth=1,
            protocol_version=2,
        )
    except Exception:
        # Fallback to a subprocess-based client that uses the system ``git``
        # binary for fetching if the builtin HTTP client fails.
        sp_client = SubprocessGitClient()
        sp_client.fetch(
            repo_url,
            repo,
            determine_wants=wants,
            filter_spec=b"blob:none",
            depth=1,
            protocol_version=2,
        )

    return repo


def collect_missing_blobs(repo: Repo, tree_sha: bytes) -> list[bytes]:
    """Return a list of blob SHAs referenced by the tree that are missing."""
    missing: list[bytes] = []
    tree = repo[tree_sha]
    for entry in tree.iteritems():
        if entry.mode & 0o40000:
            missing.extend(collect_missing_blobs(repo, entry.sha))
        else:
            if entry.sha not in repo.object_store:
                missing.append(entry.sha)
    return missing


def fetch_blobs(client, path: str, repo: Repo, blobs: list[bytes]) -> None:
    if not blobs:
        return

    def wants(refs, **kwargs):
        return blobs

    client.fetch(path, repo, determine_wants=wants, protocol_version=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch blobs for a commit")
    parser.add_argument("repo_url", help="Remote repository URL")
    parser.add_argument("commit", help="Commit SHA to fetch")
    parser.add_argument(
        "--target", help="Directory for the temporary repo", default=None
    )
    args = parser.parse_args()

    tmpdir = args.target or tempfile.mkdtemp(prefix="dulwich-partial-")
    repo = fetch_commit(args.repo_url, args.commit, tmpdir)

    commit = repo[bytes.fromhex(args.commit)]
    missing_blobs = collect_missing_blobs(repo, commit.tree)
    client, path = get_transport_and_path(args.repo_url)
    fetch_blobs(client, path, repo, missing_blobs)

    print(f"Fetched {len(missing_blobs)} blobs into {tmpdir}")


if __name__ == "__main__":
    main()
