#!/usr/bin/env python3
#
# ckatsak, Fri Oct 13 03:49:46 AM EEST 2023
# phtof,   Thu Apr 18 12:33:12 AM EEST 2024

import argparse
import json
import sys
import traceback

from benchmarks import workload_json_entries
from generator import Config, RequestGenerator, DEFAULT_SEED
from mapping import FunctionMapping
from preprocess import azure_trace_preprocess, workloads_preprocess


def parse_args(args: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    # Arguments common to all subcommands
    parser.add_argument(
        "-w",
        "--workloads-file",
        action="store",
        metavar="WORKLOADS_FILE_PATH",
        required=True,
        help="path to input Workloads file",
    )
    parser.add_argument(
        "-o",
        "--out-file",
        action="store",
        metavar="OUTPUT_FILE_PATH",
        help="path to output file; stdout if left unspecified",
    )
    parser.add_argument(
        "-V", "--version", action="version", version="faasrail-shrinkray 0.0.5"
    )
    subparsers = parser.add_subparsers(
        title="Available Subcommands",
        dest="subcmd",
        required=True,
        help="generate an experiment specification or compile a list of Workloads",
    )
    # Top-level subcommand "function"
    subparsers.add_parser(
        "functions",
        aliases=["func"],
        help="output all available Workloads as a FaaSCell-compatible JSON list",
    )
    # Top-level subcommand "trace"
    parser_trace = subparsers.add_parser(
        "trace",
        aliases=["tr"],
        help="generate a Workload trace as a FaaSCell-compatible JSON stream",  # FIXME
    )
    parser_trace.add_argument(
        "--trace-type",
        type=str,
        action="store",
        metavar="TRACE_NAME",
        choices=["azure", "huawei-prv"],
        default="azure",
        help='type of input trace { ["azure"], "huawei-prv" }',
    )
    parser_trace.add_argument(
        "--trace-dir",
        action="store",
        metavar="TRACE_DIR_PATH",
        required=True,
        help="path to directory of the input (Azure) trace files",  # FIXME: Huawei?
    )
    parser_trace.add_argument(
        "-r",
        "--request-rate",
        type=int,
        action="store",
        metavar="MAX_RPS",
        required=True,
        help="target maximum number of requests per second",
    )
    parser_trace.add_argument(
        "-d",
        "--target-duration",
        type=int,
        action="store",
        metavar="DURATION_MINUTES",
        required=True,
        help="target duration of the experiment, in minutes",
    )

    gen_mode_subparser = parser_trace.add_subparsers(
        title="Generation Mode",
        dest="gen_mode",
        required=True,
        help="supported modes for generating request specifications",
    )
    # Generation-mode subcommand "spec"
    parser_spec = gen_mode_subparser.add_parser("spec", help='FaaSRail\'s "Spec" mode')
    parser_spec.add_argument(
        "--time-scaling",
        type=str,
        action="store",
        metavar="TIME_SCALING_METHOD",
        choices=["thumbnails", "minute_range"],
        default="thumbnails",
        help='method used for scaling in time { ["thumbnails"], "minute_range" }',
    )
    parser_spec.add_argument(
        "-f",
        "--first-minute",
        type=int,
        action="store",
        metavar="FIRST_MINUTE",
        help=(
            'when "minute_range" is used as time scaling method, '
            "this parameter defines the first minute of the range"
        ),
    )
    # Generation-mode subcommand "smirnov"
    parser_smirnov = gen_mode_subparser.add_parser(
        "smirnov",
        help='FaaSRail\'s "Smirnov Transform" ("Inverse Transform Sampling") mode',
    )
    parser_smirnov.add_argument(
        "--seed",
        type=int,
        action="store",
        metavar="INTEGER",
        default=DEFAULT_SEED,
        help=f"seed randomness (default: 0x{DEFAULT_SEED:X})",
    )

    return parser.parse_args(args=args)


def validate_args(parsed_args: argparse.Namespace) -> argparse.Namespace:
    if parsed_args.subcmd not in {"functions", "func", "trace", "tr"}:
        raise RuntimeError(f'Unrecognized subcommand "{parsed_args.subcmd}"')

    if parsed_args.subcmd in {"trace", "tr"}:
        if (
            "time_scaling" in parsed_args
            and parsed_args.time_scaling == "minute_range"
            and not parsed_args.first_minute
        ):
            raise RuntimeError(
                'Time-scaling method "minute_range" requires "--first-minute"'
            )

    return parsed_args


def main(argv: list[str]):
    try:
        args = validate_args(parse_args(argv[1:]))
        fout = open(args.out_file, "w") if args.out_file else sys.stdout
        workloads = workloads_preprocess(args.workloads_file)

        if args.subcmd in {"functions", "func"}:
            json_entries = workload_json_entries(workloads)
            print(json.dumps(json_entries, indent=4), file=fout)
        elif args.subcmd in {"trace", "tr"}:
            # TODO(ckatsak): If args.trace_type == "huawei-prv", add preprocessing step!
            trace = azure_trace_preprocess(args.trace_dir)
            mapper = FunctionMapping(trace, workloads)
            parameters = Config(
                gen_mode=args.gen_mode,
                time_scaling=args.time_scaling if args.gen_mode == "spec" else "",
                max_rps=args.request_rate,
                first_minute=args.first_minute if args.gen_mode == "spec" else -42,
                target_minutes=args.target_duration,
            )
            reqgen = RequestGenerator(mapper, parameters)
            if args.gen_mode == "spec":
                specification = reqgen.spec_generate()
            elif args.gen_mode == "smirnov":
                specification = reqgen.smirnov_generate(args.seed)
            else:
                raise RuntimeError(f'Unreachable: unknown gen_mode "{args.gen_mode}"')
            specification.to_csv(fout)

        fout.close()
        return 0
    except Exception as exc:
        traceback.print_exception(exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
