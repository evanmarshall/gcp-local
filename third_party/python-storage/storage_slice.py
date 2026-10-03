# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Selected verbatim function bodies. See manifest.json for source and fixture adaptations.
import tempfile

import pytest
from google.api_core import exceptions


def test_blob_direct_write_and_read_into_file(
    shared_bucket,
    blobs_to_delete,
    service_account,
):
    payload = b"Hello World"
    blob = shared_bucket.blob("MyBuffer")
    blob.upload_from_string(payload)
    blobs_to_delete.append(blob)

    same_blob = shared_bucket.blob("MyBuffer")
    same_blob.reload()  # Initialize properties.

    with tempfile.NamedTemporaryFile() as temp_f:
        with open(temp_f.name, "wb") as file_obj:
            same_blob.download_to_file(file_obj)

        with open(temp_f.name, "rb") as file_obj:
            stored_contents = file_obj.read()

    assert stored_contents == payload


def test_blob_download_w_generation_match(
    shared_bucket,
    blobs_to_delete,
    service_account,
):
    wrong_generation_number = 6

    blob = shared_bucket.blob("MyBuffer")
    payload = b"Hello World"
    blob.upload_from_string(payload)
    blobs_to_delete.append(blob)

    same_blob = shared_bucket.blob("MyBuffer")
    same_blob.reload()  # Initialize properties.

    with tempfile.NamedTemporaryFile() as temp_f:
        with open(temp_f.name, "wb") as file_obj:
            with pytest.raises(exceptions.PreconditionFailed):
                same_blob.download_to_file(
                    file_obj, if_generation_match=wrong_generation_number
                )

            same_blob.download_to_file(
                file_obj,
                if_generation_match=blob.generation,
                if_metageneration_match=blob.metageneration,
            )

        with open(temp_f.name, "rb") as file_obj:
            stored_contents = file_obj.read()

    assert stored_contents == payload


def test_blob_crud_w_etag_match(
    shared_bucket,
    blobs_to_delete,
    file_data,
    service_account,
):
    wrong_etag = "kittens"

    blob = shared_bucket.blob("SmallFile")

    info = file_data["simple"]
    with open(info["path"], mode="rb") as to_read:
        payload = to_read.read()

    blob.upload_from_filename(info["path"])
    blobs_to_delete.append(blob)
    etag = blob.etag

    fresh_blob = shared_bucket.blob("SmallFile")

    # Exercise 'objects.get' (metadata) w/ etag match.
    with pytest.raises(exceptions.PreconditionFailed):
        fresh_blob.exists(if_etag_match=wrong_etag)

    with pytest.raises(exceptions.NotModified):
        fresh_blob.exists(if_etag_not_match=etag)

    assert fresh_blob.exists(if_etag_match=etag)
    assert fresh_blob.exists(if_etag_not_match=wrong_etag)

    with pytest.raises(exceptions.PreconditionFailed):
        fresh_blob.reload(if_etag_match=wrong_etag)

    with pytest.raises(exceptions.NotModified):
        fresh_blob.reload(if_etag_not_match=etag)

    fresh_blob.reload(if_etag_match=etag)  # no raise
    fresh_blob.reload(if_etag_not_match=wrong_etag)  # no raise

    # Exercise 'objects.get' (media) w/ etag match.
    assert fresh_blob.download_as_bytes(if_etag_match=etag) == payload

    with pytest.raises(exceptions.PreconditionFailed):
        fresh_blob.download_as_bytes(if_etag_match=wrong_etag)

    with pytest.raises(exceptions.NotModified):
        fresh_blob.download_as_bytes(if_etag_not_match=etag)

