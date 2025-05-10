#!/usr/bin/env python3
"""
Script to traverse and concatenate codebase into a single text file.
Ignores hidden files, files/folders in .gitignore, and data/docs directories.
Also supports concatenating a specific list of files.
"""

import os
from pathlib import Path
import fnmatch

OUTPUT_FILE = "codebase.txt"
ROOT_DIR = Path(__file__).parent.parent.resolve()
print(ROOT_DIR)


def parse_gitignore():
    """Parse .gitignore patterns into a list of patterns to exclude."""
    ignore_patterns = []
    try:
        with open(ROOT_DIR / ".gitignore", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith("#"):
                    # Add pattern to ignore list
                    ignore_patterns.append(line)
    except FileNotFoundError:
        pass
    return ignore_patterns


def is_ignored_by_gitignore(path, gitignore_patterns):
    """
    Check if a path matches any pattern in .gitignore
    Handles directory-specific patterns correctly.
    """
    if not gitignore_patterns:
        return False

    # Get the relative path from the root directory
    rel_path = str(path.relative_to(ROOT_DIR))

    # Handle both files and directories
    is_dir = path.is_dir()

    for pattern in gitignore_patterns:
        # Skip comment lines
        if pattern.startswith("#"):
            continue

        # Handle negation (patterns that start with !)
        if pattern.startswith("!"):
            # If the pattern is negated, the file is explicitly NOT ignored
            if fnmatch.fnmatch(rel_path, pattern[1:]) or (
                is_dir and fnmatch.fnmatch(f"{rel_path}/", pattern[1:])
            ):
                return False
            continue

        # Handle directory-specific patterns (ending with /)
        if pattern.endswith("/"):
            if is_dir and (
                fnmatch.fnmatch(rel_path, pattern[:-1])
                or fnmatch.fnmatch(f"{rel_path}/", pattern)
            ):
                return True
            continue

        # Handle glob patterns with wildcards
        if fnmatch.fnmatch(rel_path, pattern) or (
            is_dir and fnmatch.fnmatch(f"{rel_path}/", pattern)
        ):
            return True

        # Handle matching at subdirectory level (any subdirectory can match)
        path_parts = rel_path.split(os.sep)
        for i in range(len(path_parts)):
            subpath = os.sep.join(path_parts[i:])
            if fnmatch.fnmatch(subpath, pattern):
                return True

    return False


def should_ignore(path, gitignore_patterns):
    """Check if a path should be ignored."""
    # Skip the output file itself
    if path.name == OUTPUT_FILE:
        return True

    # Always skip hidden files and directories (those that start with .)
    if path.name.startswith("."):
        return True

    # Explicitly ignore data and docs directories
    if path.name in [
        "data",
        "docs",
        "pyproject.toml",
        "uv.lock",
        "poetry.lock",
        "README.md",
    ]:
        return True

    # Skip files matching gitignore patterns
    if is_ignored_by_gitignore(path, gitignore_patterns):
        return True

    return False


def process_codebase():
    """Process the codebase and write to output file."""
    gitignore_patterns = parse_gitignore()

    with open(ROOT_DIR / OUTPUT_FILE, "w", encoding="utf-8") as out_file:
        for root, dirs, files in os.walk(ROOT_DIR):
            root_path = Path(root)

            # Remove directories we want to skip
            dirs[:] = [
                d for d in dirs if not should_ignore(root_path / d, gitignore_patterns)
            ]

            for file in files:
                file_path = root_path / file

                if not should_ignore(file_path, gitignore_patterns):
                    try:
                        # Get relative path from root directory
                        rel_path = file_path.relative_to(ROOT_DIR)

                        # Write file header
                        out_file.write(f"--------------------- {rel_path}\n\n")

                        # Write file contents
                        with open(
                            file_path, "r", encoding="utf-8", errors="replace"
                        ) as in_file:
                            content = in_file.read()
                            out_file.write(content)

                        # Add separator after file content
                        out_file.write("\n\n")
                    except (UnicodeDecodeError, IsADirectoryError):
                        # Skip binary files or directories
                        out_file.write(
                            f"--- {rel_path}\n\n[Binary file or error reading file]\n\n"
                        )
                    except Exception as e:
                        out_file.write(f"--- {rel_path}\n\n[Error: {str(e)}]\n\n")


def process_specific_files(file_list, output_file=None):
    """Process only specific files from the file_list and write to output file."""
    output_path = ROOT_DIR / (output_file or OUTPUT_FILE)

    with open(output_path, "w", encoding="utf-8") as out_file:
        for file_path in file_list:
            # Convert string path to Path object
            full_path = ROOT_DIR / file_path

            try:
                # Write file header
                out_file.write(f"--------------------- {file_path}\n\n")

                # Write file contents
                with open(
                    full_path, "r", encoding="utf-8", errors="replace"
                ) as in_file:
                    content = in_file.read()
                    out_file.write(content)

                # Add separator after file content
                out_file.write("\n\n")
            except (UnicodeDecodeError, IsADirectoryError):
                # Skip binary files or directories
                out_file.write(
                    f"--- {file_path}\n\n[Binary file or error reading file]\n\n"
                )
            except FileNotFoundError:
                out_file.write(f"--- {file_path}\n\n[Error: File not found]\n\n")
            except Exception as e:
                out_file.write(f"--- {file_path}\n\n[Error: {str(e)}]\n\n")

    return output_path


if __name__ == "__main__":
    # Example usage:
    affected_files = [
        "docker-compose.yml",
        "src/common/validation/base_service.py",
        "src/data_collection/social_media/reddit/validation/service.py",
        "src/data_collection/news/validation/validation_service.py",
        "src/data_collection/market_data/collectors/__init__.py",
        "src/data_collection/market_data/collectors/stock_market_collector.py",
        "src/data_processing/common/base_processor.py",
        "src/data_processing/stock/stock_processor.py",
        "src/data_processing/market_data/feature_eng/stock_features.py",
        "src/data_processing/main.py",
        "src/common/messaging/kafka_consumer.py",
        "src/common/messaging/kafka_producer.py",
        "src/data_collection/news/collectors/newsapi_collector.py",
        "src/data_collection/social_media/reddit/collectors/reddit_collector.py",
    ]
    output_path = process_specific_files(affected_files, "affected_files.txt")
    print(f"Specific files concatenated to {output_path}")
    print(f"Starting to process codebase at {ROOT_DIR}")
    process_codebase()
    print(f"Codebase concatenated to {ROOT_DIR / OUTPUT_FILE}")