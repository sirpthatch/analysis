from pathlib import Path
import pandas as pd
import click
import importlib
import shutil

class EtlModule(object):
    """
        Base Class for EtlModule calculations

        Some high level semantics:
        * Operates as a build system semantic for data processing
        * If pointed at a directory of raw files, it will look to see
          what partitions are available in that data and attempt to write
          those files indexed by the partitions in the output directory
        * It will not overwrite any existing partition - if the partition is
          already written, it will skip it
    """

    def partition(self, file_paths:list[Path]) -> dict[Path, list[str]]:
        """
            Should return the list of partitions that are present in the supplied
            file_path objects.  Each path can map to multiple paritions, so this returns
            a map of Path -> Seq[Partition Name]
        """
        raise NotImplementedError("partition not implemented")
    
    def process_files(self, file_paths:list[Path]) -> dict[str, pd.DataFrame]:
        """
            Should process all of the data in the supplied file_paths and turn them into
            type validated DataFrames grouped by their partition.  Each returned data frame
            should be considered an independent partition.

            If the dataframe is a single partition, then the dict can have a single key (by 
            convention use "global")
        """
        raise NotImplementedError("process_files not implemented")

    def verify_consistent(self, file_paths:list[Path]) -> None:
        for p in file_paths:
            if not isinstance(p, Path):
                raise TypeError(f"file_paths must contain pathlib.Path objects, got {type(p)}")
            if not p.exists():
                raise FileNotFoundError(f"{p} does not exist")
            if p.suffix.lower() != ".csv":
                raise ValueError(f"{p} is not a CSV file")

        base_cols = None
        base_dtypes = None

        for i, p in enumerate(file_paths):
            df = pd.read_csv(p)
            if i == 0:
                base_cols = list(df.columns)
                base_dtypes = df.dtypes.astype(str).to_dict()
            else:
                cols = list(df.columns)
                if cols != base_cols:
                    missing = [c for c in base_cols if c not in cols]
                    extra = [c for c in cols if c not in base_cols]
                    raise ValueError(f"Schema mismatch in {p}: column order/names differ. missing={missing}, extra={extra}")
                dtypes = df.dtypes.astype(str).to_dict()
                dtype_mismatches = {c: (base_dtypes[c], dtypes[c]) for c in base_cols if base_dtypes.get(c) != dtypes.get(c)}
                if dtype_mismatches:
                    raise ValueError(f"Dtype mismatch in {p}: {dtype_mismatches}")


@click.group()
def cli():
    """ETL data processing tool."""
    pass


@cli.command()
@click.option('--src', required=True, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
              help='Path to directory containing raw source files')
@click.option('--out', required=True, type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
              help='Path to output directory')
@click.option('--name', required=True, type=str, help='Module name to use')
def ingest(src: Path, out: Path, name: str):
    """Process raw source files and generate output partitions."""
    click.echo("="*50)
    click.echo(f"Processing files from: {src}")
    click.echo(f"Output directory: {out}")
    click.echo(f"Module name: {name}")
    click.echo("\n")

    available_paths = [p for p in src.iterdir() if p.is_file() and not p.name.startswith(".")]

    try:
        # Split module path from class name (e.g., "process_oath.OathEtlModule")
        if '.' in name:
            module_path, class_name = name.rsplit('.', 1)
            module = importlib.import_module(module_path)
            etl_class = getattr(module, class_name)
        else:
            # If no dot, assume it's just a module name with a default class
            module = importlib.import_module(name)
            etl_class = module
    except (ModuleNotFoundError, AttributeError) as exc:
        raise click.ClickException(f"Unable to import '{name}': {exc}") from exc

    # Instantiate the ETL class
    etl_instance = etl_class()


    existing_partition_dirs:list[Path] = [d for d in out.iterdir() if d.is_dir()] if out.exists() else []
    existing_partitions = set([d.name for d in existing_partition_dirs])
    
    available_partitions = etl_instance.partition(available_paths)
    
    paths_to_process:list[Path] = []
    paths_to_skip:list[Path] = []
    for path, containing_partitions in available_partitions.items():
        unwritten_partitions:list[str] = [c for c in containing_partitions if c not in existing_partitions]
        if len(unwritten_partitions) > 0 and not path.name.startswith("."):
            paths_to_process.append(path)
        else:
            paths_to_skip.append(path)

    
    partitions_to_write = set()
    for path, containing_partitions in available_partitions.items():
        for part in containing_partitions:
            if part not in existing_partitions:
                partitions_to_write.add(part)

    click.echo(f"Paths to process (len: {len(paths_to_process)}):")
    for p in paths_to_process:
        click.echo(f"  - {p}")

    click.echo(f"Paths to skip (len {len(paths_to_skip)}):")
    for p in paths_to_skip:
        click.echo(f"  - {p}")

    click.echo(f"Existing partitions (len {len(existing_partitions)}):")
    for name in sorted(existing_partitions):
        click.echo(f"  - {name}")

    click.echo(f"Expected partitions that will be written (len {len(partitions_to_write)}):")
    for name in sorted(partitions_to_write):
        click.echo(f"  - {name}")
    click.echo("="*50)
    click.echo("\n\n")

    data_frames = etl_instance.process_files(paths_to_process)
   
    for part, frame in data_frames.items():
        if part in existing_partitions:
            continue

        output_path = out / part / "data.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(output_path, index=False)


@cli.command()
@click.option('--src', required=True, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
              help='Path to directory containing raw source files')
@click.option('--out', required=True, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
              help='Path to output directory')
@click.option('--name', required=True, type=str, help='Module name to use')
@click.option('--dry-run', is_flag=True, help='Show what would be deleted without actually deleting')
def clean(src: Path, out: Path, name: str, dry_run: bool):
    """Clean output partitions that would be generated from source files."""
    click.echo(f"Cleaning partitions from: {out}")
    click.echo(f"Based on source files in: {src}")
    click.echo(f"Module name: {name}")

    available_paths = [p for p in src.iterdir() if p.is_file()]

    try:
        # Split module path from class name (e.g., "process_oath.OathEtlModule")
        if '.' in name:
            module_path, class_name = name.rsplit('.', 1)
            module = importlib.import_module(module_path)
            etl_class = getattr(module, class_name)
        else:
            # If no dot, assume it's just a module name with a default class
            module = importlib.import_module(name)
            etl_class = module
    except (ModuleNotFoundError, AttributeError) as exc:
        raise click.ClickException(f"Unable to import '{name}': {exc}") from exc

    # Instantiate the ETL class
    etl_instance = etl_class()
    available_partitions = etl_instance.partition(available_paths)

    # Get all partition names that would be generated from the source files
    partitions_to_clean = set()
    for path, containing_partitions in available_partitions.items():
        partitions_to_clean.update(containing_partitions)

    # Find which of these partitions actually exist in the output directory
    existing_to_delete = []
    for partition_name in partitions_to_clean:
        partition_path = out / partition_name
        if partition_path.exists() and partition_path.is_dir():
            existing_to_delete.append(partition_path)

    if not existing_to_delete:
        click.echo("No partitions found to clean.")
        return

    click.echo(f"\nPartitions to {'be deleted' if not dry_run else 'delete (dry-run)'}:")
    for part_path in existing_to_delete:
        click.echo(f"  - {part_path.name}")

    if dry_run:
        click.echo("\nDry-run mode: no files were deleted.")
        return

    if click.confirm(f"\nAre you sure you want to delete {len(existing_to_delete)} partition(s)?"):
        for part_path in existing_to_delete:
            click.echo(f"Deleting {part_path.name}...")
            shutil.rmtree(part_path)
        click.echo(f"\nSuccessfully deleted {len(existing_to_delete)} partition(s).")
    else:
        click.echo("Clean operation cancelled.")


if __name__ == "__main__":
    cli()

    