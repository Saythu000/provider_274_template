import os
import sys
import shutil
from pathlib import Path

# Setup workspace environment paths
file_dir = Path(__file__).resolve().parent
root_dir = file_dir.parent.parent
print("root directory: ", root_dir)

sys.path.append(str(root_dir))

from Shared.EDIProcessing.ediprocessing import EDIProcessor
from Shared.EDIProcessing.csvconverter import CSVConverter
from DimProvider.EDIProcessing.mapper import CSVSchemaMapper


def main():
    mapper = CSVSchemaMapper()
    schemas_dir = str(root_dir / "DimProvider/Bronze/Schema")

    # Pipeline: Directory (EDI 274) -> provider_hierarchy.csv
    source_274  = root_dir / "source/274"
    pending_274 = source_274 / "pending/provider_hierarchy1.txt"

    # Fallback: check for any .txt file if the standard filename does not exist
    if not pending_274.exists():
        pending_files = list((source_274 / "pending").glob("*.txt"))
        if pending_files:
            pending_274 = pending_files[0]

    output_274  = root_dir / "temp/274/provider_hierarchy1.csv"

    print("\n=== Processing Directory (EDI 274) ===")
    try:
        if not pending_274.exists():
            print(f"No pending 274 file found at: {pending_274}. Skipping.")
        else:
            inprogress_dir = source_274 / "inprogress"
            processed_dir  = source_274 / "processed"
            failed_dir     = source_274 / "failed"
            active_file    = pending_274

            # Transition: Pending -> Inprogress
            os.makedirs(inprogress_dir, exist_ok=True)
            inprogress_file = inprogress_dir / active_file.name
            print(f"Moving file to execution phase: {inprogress_file}")
            shutil.move(str(active_file), str(inprogress_file))
            active_file = inprogress_file

            # Parse -> Map -> Convert
            structured_274     = EDIProcessor().parse(str(active_file))
            hierarchy_records  = mapper.map_hierarchy(structured_274)
            os.makedirs(output_274.parent, exist_ok=True)
            
            CSVConverter(schemas_dir=schemas_dir).convert_hierarchy(
                mapped_hierarchy=hierarchy_records,
                output_csv_path=str(output_274)
            )
            print(f"Directory processing complete. Output: {output_274}")

            # Transition: Inprogress -> Processed
            os.makedirs(processed_dir, exist_ok=True)
            processed_file = processed_dir / active_file.name
            shutil.move(str(active_file), str(processed_file))
            print(f"Moving file to completion phase: {processed_file}")

    except Exception as e:
        print(f"Directory (274) pipeline failed: {e}")
        raise e


if __name__ == "__main__":
    main()
