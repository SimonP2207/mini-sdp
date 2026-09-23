import os
import subprocess

from ..data.models import ObservingBlock, RawDataset


def estimate_obs_dataset_size_bytes() -> int:
    """Estimate a measurement set (pre-observation) size on disk in bytes"""
    # Actual logic for estimating size based on observational parameters here
    # which will use observational parameters such as bandwidth, channel size,
    # integration time, total observation time etc. supplied as relevant (new)
    # model instance
    return 628_273_152


class DataObserver:
    def observe(self, obs_block: ObservingBlock, out_dataset: RawDataset):
        local_data_dir = str(out_dataset.path.resolve().parent)
        ms_name = out_dataset.path.name
        container_name = f"sdp-observer-ob-{obs_block.id}"

        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "--user", f"{os.getuid()}:{os.getgid()}",
                "--name", container_name,
                "-v",
                f"{local_data_dir}:/data",
                "docker.io/pw410/ska-sdp-mock:0.1",
                "/scripts/generate_visibilities.sh",
                f"/data/{ms_name}",
            ],
            capture_output=True, text=True, check=False
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"docker container failed with status {result.returncode}: "
                f"{result.stderr}"
            )
