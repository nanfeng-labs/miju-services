#!/usr/bin/env python3
"""Build and run the real JVM/UDP example with explicit build and evidence paths."""
import argparse
import json
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--java-home", required=True, type=Path)
parser.add_argument("--target-dir", required=True, type=Path)
parser.add_argument("--output", required=True, type=Path)
parser.add_argument("--toolchain", default="1.96.0")
args = parser.parse_args()
args.java_home = args.java_home.resolve()
here = Path(__file__).resolve().parent
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=False)
target = args.target_dir.resolve()
env = dict(os.environ, CARGO_TARGET_DIR=str(target))
commands = []


def run(name, command, timeout):
    print(name, flush=True)
    result = subprocess.run(command, cwd=output, env=env, capture_output=True, text=True, timeout=timeout)
    (output / (name + ".stdout.log")).write_text(result.stdout)
    (output / (name + ".stderr.log")).write_text(result.stderr)
    commands.append({"name": name, "command": [str(v) for v in command], "exit_code": result.returncode})
    (output / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    if result.returncode:
        raise SystemExit(name + " failed; inspect " + str(output))


run("build", ["cargo", "+" + args.toolchain, "build", "--locked", "--manifest-path", str(here / "Cargo.toml")], 1200)
classes = output / "classes"
classes.mkdir()
run("javac", [str(args.java_home / "bin/javac"), "--release", "17", "-Xlint:all", "-Werror", "-d", str(classes), *map(str, sorted((here / "java").rglob("*.java")))], 60)
run("java", [str(args.java_home / "bin/java"), "-Xcheck:jni", "-ea", "-Djava.library.path=" + str(target / "debug"), "-cp", str(classes), "com.example.Main"], 60)
print("PASS: real JVM UDP echo, cleanup, port release, and second generation; " + str(output))
