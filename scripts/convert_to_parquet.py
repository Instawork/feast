import os
import sys

import pandas as pd


def csv_to_parquet(csv_file, output_file=None):
    """
    Convert CSV file to Parquet format

    Args:
        csv_file: Path to input CSV file
        output_file: Path to output Parquet file (optional)
    """
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"❌ Error: File '{csv_file}' not found")
        return False

    # Generate output filename if not provided
    if output_file is None:
        output_file = csv_file.replace(".csv", ".parquet")

    try:
        print(f"📖 Reading CSV: {csv_file}")

        # Try different encodings in order of likelihood
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1", "utf-16"]
        df = None

        # Check pandas version for compatibility
        pandas_version = pd.__version__
        use_on_bad_lines = tuple(map(int, pandas_version.split(".")[:2])) >= (1, 3)

        # Try different reading strategies
        reading_strategies = [
            # Strategy 1: Standard C engine with common options
            {
                "engine": "c",
                "sep": ",",
                "low_memory": False,
            },
            # Strategy 2: Python engine (more forgiving)
            {
                "engine": "python",
                "sep": ",",
            },
            # Strategy 3: Try tab delimiter
            {
                "engine": "python",
                "sep": "\t",
            },
            # Strategy 4: Auto-detect delimiter
            {
                "engine": "python",
                "sep": None,
            },
        ]

        for encoding in encodings:
            for strategy in reading_strategies:
                try:
                    read_kwargs = {
                        "encoding": encoding,
                        **strategy,
                    }
                    # Add bad line handling based on pandas version
                    if use_on_bad_lines:
                        read_kwargs["on_bad_lines"] = "skip"
                    else:
                        read_kwargs["error_bad_lines"] = False
                        read_kwargs["warn_bad_lines"] = False

                    # Remove sep=None if present (pandas will auto-detect)
                    if read_kwargs.get("sep") is None:
                        read_kwargs.pop("sep")

                    df = pd.read_csv(csv_file, **read_kwargs)

                    if encoding != "utf-8":
                        print(f"   ℹ️  Detected encoding: {encoding}")
                    if strategy["engine"] == "python":
                        print("   ℹ️  Used Python engine (more forgiving parser)")
                    break
                except (UnicodeDecodeError, pd.errors.ParserError, ValueError):
                    continue
                except Exception:
                    # For other errors, try next strategy
                    continue

            if df is not None:
                break

        if df is None:
            print(
                "❌ Error: Could not read file with any combination of encodings and strategies"
            )
            return False

        print(f"   ✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
        print(f"   ✓ Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        print(f"\n💾 Writing Parquet: {output_file}")
        df.to_parquet(output_file, index=False, compression="snappy")

        # Get file sizes
        csv_size = os.path.getsize(csv_file) / 1024**2
        parquet_size = os.path.getsize(output_file) / 1024**2
        compression_ratio = (1 - parquet_size / csv_size) * 100

        print(f"   ✓ CSV size: {csv_size:.2f} MB")
        print(f"   ✓ Parquet size: {parquet_size:.2f} MB")
        print(f"   ✓ Compression: {compression_ratio:.1f}% smaller")
        print(f"\n✅ Success! Created: {output_file}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python csv_to_parquet.py <input.csv> [output.parquet]")
        print("\nExample:")
        print("  python csv_to_parquet.py data/myfile.csv")
        print("  python csv_to_parquet.py data/myfile.csv data/output.parquet")
        sys.exit(1)

    csv_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    csv_to_parquet(csv_file, output_file)
