# Contributing to Rafflesia-Ambush

Thank you for your interest in contributing to the Rafflesia-Ambush framework! This document provides guidelines for contributing to the project.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR-USERNAME/Rafflesia-Ambush.git`
3. Create a new branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Test your changes
6. Submit a pull request

## Development Setup

### For Python Apps

```bash
cd apps/qlib-training-data
pip install -r requirements.txt
pip install -e ".[dev]"  # Install development dependencies
```

### Running Tests

```bash
# For Python apps
python -m unittest discover tests/

# Or with pytest (if installed)
pytest tests/
```

## Code Style

### Python

- Follow PEP 8 style guide
- Use type hints where applicable
- Maximum line length: 88 characters (Black formatter default)
- Use docstrings for all public functions and classes

Example:
```python
def process_data(data: pd.DataFrame, config: Dict) -> pd.DataFrame:
    """
    Process data according to configuration.
    
    Args:
        data: Input data
        config: Processing configuration
        
    Returns:
        Processed data
    """
    # Implementation
    pass
```

### Commit Messages

- Use clear and descriptive commit messages
- Start with a verb in present tense (Add, Fix, Update, etc.)
- Keep the first line under 72 characters
- Add detailed description if necessary

Example:
```
Add momentum indicator calculation

- Implement 5-day and 10-day momentum calculations
- Add unit tests for momentum indicators
- Update documentation
```

## Adding a New App

When adding a new application to the framework:

1. Create a new directory under `apps/`:
   ```
   apps/your-app-name/
   ├── src/
   ├── config/
   ├── tests/
   ├── examples/
   ├── README.md
   └── requirements.txt (or equivalent)
   ```

2. Follow the structure of existing apps (e.g., `qlib-training-data`)

3. Include:
   - Comprehensive README
   - Unit tests
   - Configuration examples
   - Usage examples
   - Dependencies file

4. Update the main README.md with:
   - Description of the new app
   - Integration points with other apps
   - Quick start guide

## Pull Request Process

1. **Before submitting**:
   - Ensure all tests pass
   - Update documentation
   - Follow code style guidelines
   - Add tests for new features

2. **PR Description**:
   - Describe what the PR does
   - Link to related issues
   - Include screenshots for UI changes
   - List breaking changes (if any)

3. **Review Process**:
   - Address reviewer feedback
   - Keep the PR focused (one feature/fix per PR)
   - Rebase if necessary

## Testing Guidelines

### Unit Tests

- Write tests for all new functionality
- Aim for high code coverage (>80%)
- Use mocks for external dependencies
- Test edge cases and error conditions

Example:
```python
import unittest
from unittest.mock import Mock, patch

class TestDataProcessor(unittest.TestCase):
    def test_feature_generation(self):
        # Test implementation
        pass
```

### Integration Tests

- Test interactions between components
- Use realistic data samples
- Document test data requirements

## Documentation

### Code Documentation

- Use clear and descriptive docstrings
- Document parameters and return values
- Include usage examples in docstrings

### README Updates

- Keep README files up to date
- Include installation instructions
- Add usage examples
- Document configuration options

## Security

- Never commit secrets, API keys, or credentials
- Use environment variables for sensitive data
- Report security vulnerabilities privately
- Follow security best practices for your language

## Questions?

- Open an issue for discussion
- Check existing issues and pull requests
- Review the architecture documentation in `docs/`

## Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Help others learn and grow
- Focus on what's best for the community

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Recognition

Contributors will be recognized in:
- The project README
- Release notes
- Git commit history

Thank you for contributing to Rafflesia-Ambush! 🎉
