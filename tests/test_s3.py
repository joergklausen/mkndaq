from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any

import pytest

from mkndaq.utils.s3fsc import S3FSC


# ---- A tiny fake S3 client so we don't hit the network
@dataclass
class Call:
    Filename: str
    Bucket: str
    Key: str


@dataclass
class FakeS3Client:
    calls: List[Call] = field(default_factory=list)

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        # Minimal behavior: store the call; optionally verify local file exists
        if not Path(Filename).is_file():
            raise FileNotFoundError(Filename)
        self.calls.append(Call(Filename=Filename, Bucket=Bucket, Key=Key))


@pytest.fixture
def minimal_s3_config() -> Dict[str, Any]:
    # Keep it self-contained and not dependent on any real credentials
    return {
        "s3": {
            "endpoint_url": "https://test.invalid",
            "aws_s3_bucket_name": "unit-test-bucket",
            "aws_region": "eu-central-1",
            "aws_access_key_id": "FAKE",
            "aws_secret_access_key": "FAKE",
        }
    }


def test_upload_simple_file(tmp_path, minimal_s3_config):
    # Arrange: create a temp file on the fly
    f = tmp_path / "hello.world"
    f.write_text("hello, world!\n")

    fake = FakeS3Client()
    client = S3FSC(config=minimal_s3_config, s3_client=fake)  # no prefix

    # Act
    key = client.upload(f)

    # Assert
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call.Bucket == "unit-test-bucket"
    assert call.Filename == str(f)
    assert call.Key == "hello.world"
    assert key == "hello.world"


def test_upload_with_prefixes(tmp_path, minimal_s3_config):
    f = tmp_path / "hello.world"
    f.write_text("hello, world!\n")

    fake = FakeS3Client()
    # constructor-level prefix (e.g., a logical root)
    client = S3FSC(config=minimal_s3_config, s3_client=fake)

    # method-level key_prefix (e.g., a date path)
    key = client.upload(f, key_prefix="staging/test")

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call.Bucket == "unit-test-bucket"
    assert call.Key == "staging/test/hello.world"
    assert key == "staging/test/hello.world"


def test_upload_missing_file_raises(minimal_s3_config):
    fake = FakeS3Client()
    client = S3FSC(config=minimal_s3_config, s3_client=fake)

    with pytest.raises(FileNotFoundError):
        client.upload("does/not/exist.txt")
