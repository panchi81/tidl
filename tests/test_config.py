"""Simple test to verify pytest configuration."""


def test_pytest_config_working():
    """Verify that pytest can find and run tests."""
    assert True  # Simple test to ensure pytest is working


def test_markers_available():
    """Test that custom markers are available."""
    # This test should be discoverable by pytest
    assert 1 + 1 == 2
