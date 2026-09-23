import logging
import subprocess

from ..data.models import ProcessedDataset, ProcessingBlock, RawDataset


def estimate_processed_dataset_size_bytes() -> int:
    """Estimate a processed dataset (pre-processed) size on disk in bytes"""
    # Actual logic for estimating size based on processing parameters here
    # which will use relevant parameters for WSClean such as pixel number,
    # channel number etc. supplied as relevant (new) model instance
    return 21_012_480


class DataProcessor:
    def __init__(self, logger: logging.Logger | None = None):
        self._logger = logger if logger is not None else logging.getLogger(__name__)

    def process(
        self,
        processing_block: ProcessingBlock,
        raw_dataset: RawDataset,
        processed_dataset: ProcessedDataset,
        dataset_prefix: str | None = None
    ):
        raw_data_dir = str(processed_dataset.path.resolve().parent)
        processed_data_dir = str(processed_dataset.path.resolve())
        ms_name = raw_dataset.path.name
        container_name = f"sdp-process-pb-{processing_block.id}"
        
        if dataset_prefix is None:
            dataset_prefix = raw_dataset.path.stem

        clargs = [
            "docker", "run", "--rm", "-v",
            f"{raw_data_dir}:/raw_data",
            "--name", container_name,
            "-v",
            f"{processed_data_dir}:/processed_data",
            "docker.io/pw410/ska-sdp-mock:0.1",
            "/scripts/process_visibilities.sh",
            f"/raw_data/{ms_name}",
            f"/processed_data/{dataset_prefix}"
        ]

        result = subprocess.run(clargs, capture_output=True, text=True, check=False)

        if result.returncode != 0:
            raise RuntimeError(
                f"docker container failed with status {result.returncode}: "
                f"{result.stderr}"
            )
