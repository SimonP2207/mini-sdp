"""Data Archiver: copies QA-approved data products to the Product Archive"""
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from ..data.models import ProcessedDataset


class Archive(ABC):
    @abstractmethod
    def write(self, product: Path, dest_rel: Path) -> None:
        """Copy a single product file to ``dest_rel`` within the archive."""


class LocalArchive(Archive):
    """Archive directory located locally"""
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def write(self, product: Path, dest_rel: Path) -> None:
        dest = self.root / dest_rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(product, dest)


class DataArchiver:
    def __init__(self, archive: Archive):
        self._archive = archive

    def archive(self, dataset: ProcessedDataset) -> None:
        """Copy every product of a processed dataset into the archive"""
        path = Path(dataset.path)
        products = sorted(p for p in path.iterdir() if p.is_file())
        dest_dir = Path(f"dataset-{dataset.id:04d}") if dataset.id is not None else Path(path.name)
        for product in products:
            self._archive.write(product, dest_dir / product.name)
