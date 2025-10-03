import click
import os
from pathlib import Path
import csv

@click.command()
@click.argument('input_files', nargs=-1, type=click.Path(exists=True, dir_okay=False))
@click.option('--output-dir', '-o', required=True, type=click.Path(file_okay=False, writable=True), help='Directory to write normalized files')
def normalize(input_files, output_dir):
    """
    Normalize a set of INPUT_FILES and write results to OUTPUT_DIR.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    def partition(record:dict[str]) -> str:
        date_worked = record["date_worked"]
        year = date_worked.split("/")[2]
        return year

    def key(record:dict[str]) -> str:
        return "".join(record.values())
    
    partition_keymap = dict()

    writers = dict()
    f_handles = []
    existing_partitions = set([f.split(".")[0] for f in os.listdir(output_dir)])
    try:
        for file_path in input_files:
            input_path = Path(file_path)
            with input_path.open('r') as infile:
                reader = csv.DictReader(infile)
                for line in reader:
                    part = partition(line)
                    k = key(line)

                    if part in existing_partitions:
                        continue

                    if part not in partition_keymap:
                        partition_keymap[part] = set()
                        write_h = open(os.path.join(output_dir, f"{part}.csv"),"w")
                        f_handles.append(write_h)
                        writer = csv.DictWriter(write_h, reader.fieldnames)
                        writer.writeheader()
                        writers[part] = writer


                    if k in partition_keymap[part]:
                        continue
                    partition_keymap[part].add(k)

                    writers[part].writerow(line)
    finally:
        for handle in f_handles:
            handle.close()

    click.echo(f"Normalized {input_path} -> {output_dir}, wrote {len(writers)} partitions")

if __name__ == '__main__':
    normalize()