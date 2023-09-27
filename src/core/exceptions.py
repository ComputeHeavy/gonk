# Copyright 2023 - Compute Heavy Industries Incorporated
# This work is released, distributed, and licensed under AGPLv3.

class ValidationError(Exception):
    """Event validation exception."""
    pass

class StorageError(Exception):
    """Exception for storage related issues."""
    pass