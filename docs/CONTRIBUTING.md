# Contributing to DeltaFlow

Thank you for your interest in contributing to DeltaFlow! We welcome contributions from the community and are grateful for your support in making this project better.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Environment Setup](#development-environment-setup)
- [Code Style Guidelines](#code-style-guidelines)
- [Testing Requirements](#testing-requirements)
- [Pull Request Process](#pull-request-process)
- [Commit Message Conventions](#commit-message-conventions)
- [Adding New Data Sources](#adding-new-data-sources)
- [Documentation Standards](#documentation-standards)
- [Community and Communication](#community-and-communication)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](../CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Create a new branch for your feature or bugfix
4. Make your changes
5. Submit a pull request

## Development Environment Setup

### Prerequisites

- Python 3.9 or higher
- Google Cloud SDK (for Dataflow testing)
- Docker (for container builds)
- Git

### Setting Up Your Local Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/deltaflow.git
   cd deltaflow
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Install development dependencies:**
   ```bash
   pip install pytest pytest-cov black ruff mypy
   ```

5. **Verify installation:**
   ```bash
   python main.py --help
   ```

### Running Locally

Test the pipeline locally using DirectRunner:

```bash
python main.py \
  --runner=DirectRunner \
  --data_source=postgresql \
  --postgresql_host=localhost \
  --postgresql_database=testdb \
  --postgresql_username=user \
  --postgresql_password=pass \
  --postgresql_query="SELECT * FROM test_table" \
  --destination_bigquery_project=your-project \
  --destination_bigquery_dataset=test_dataset \
  --destination_bigquery_table=test_table
```

## Code Style Guidelines

We follow Python best practices and use automated tools to enforce code quality.

### Style Standards

- **PEP 8**: Follow Python's official style guide
- **Black**: Use Black formatter with default settings (88 character line length)
- **Ruff**: Use Ruff for fast linting
- **Type hints**: Use type annotations where appropriate

### Formatting Your Code

Before submitting a PR, format your code:

```bash
# Format with Black
black main.py

# Run Ruff linter
ruff check main.py

# Fix auto-fixable issues
ruff check --fix main.py
```

### Code Quality Checklist

- [ ] Code follows PEP 8 guidelines
- [ ] All functions have docstrings
- [ ] Type hints are used for function signatures
- [ ] No unused imports or variables
- [ ] Code is formatted with Black
- [ ] Ruff linting passes without errors

## Testing Requirements

All contributions should include appropriate tests.

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=main --cov-report=html --cov-report=term

# Run specific test file
pytest tests/test_smart_sync.py
```

### Coverage Requirements

- Minimum coverage: **80%** for new code
- Critical components (Smart Sync, Auto-Schema) should have **90%+** coverage
- Include both unit tests and integration tests where appropriate

### Writing Tests

1. **Unit Tests**: Test individual functions and DoFn classes in isolation
   ```python
   def test_sanitize_for_bigquery_json():
       result = sanitize_for_bigquery_json({'key': b'bytes'})
       assert isinstance(result['key'], str)
   ```

2. **Integration Tests**: Test complete pipeline flows using DirectRunner
   ```python
   def test_postgresql_to_bigquery_pipeline():
       # Test end-to-end pipeline execution
       pass
   ```

3. **Mock External Services**: Use mocks for database connections
   ```python
   @patch('psycopg2.connect')
   def test_postgresql_reader(mock_connect):
       # Mock database connection
       pass
   ```

### Test Organization

Create test files in a `tests/` directory:
- `tests/test_readers.py` - Data source reader tests
- `tests/test_smart_sync.py` - Smart Sync logic tests
- `tests/test_auto_schema.py` - Auto-Schema detection tests
- `tests/test_transformations.py` - Data transformation tests
- `tests/test_validation.py` - Data validation tests

## Pull Request Process

### Before Submitting

1. **Update your branch:**
   ```bash
   git checkout main
   git pull upstream main
   git checkout your-feature-branch
   git rebase main
   ```

2. **Run all checks:**
   ```bash
   black main.py
   ruff check main.py
   pytest --cov=main
   ```

3. **Update documentation:**
   - Update CLAUDE.md if adding new features
   - Update README.md if changing user-facing functionality
   - Add inline comments for complex logic

### Submitting Your PR

1. **Push to your fork:**
   ```bash
   git push origin your-feature-branch
   ```

2. **Create a Pull Request** on GitHub with:
   - Clear title describing the change
   - Detailed description of what changed and why
   - Reference any related issues
   - Screenshots/examples if applicable

3. **PR Description Template:**
   ```markdown
   ## Description
   Brief description of changes

   ## Type of Change
   - [ ] Bug fix
   - [ ] New feature
   - [ ] Breaking change
   - [ ] Documentation update

   ## Testing
   - [ ] Unit tests added/updated
   - [ ] Integration tests added/updated
   - [ ] Manual testing completed

   ## Checklist
   - [ ] Code follows style guidelines
   - [ ] Self-review completed
   - [ ] Comments added for complex logic
   - [ ] Documentation updated
   - [ ] Tests pass locally
   - [ ] Coverage maintained at 80%+
   ```

### Review Process

- Maintainers will review your PR within 3-5 business days
- Address any feedback or requested changes
- Once approved, a maintainer will merge your PR

## Commit Message Conventions

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification.

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, no logic change)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

### Examples

```bash
feat(smart-sync): add support for custom timestamp columns

Allows users to specify a custom timestamp column for Smart Sync
instead of defaulting to 'updated_at'.

Closes #123
```

```bash
fix(bigquery): handle null bytes in JSON fields

Sanitize null bytes before BigQuery write to prevent
validation errors.
```

```bash
docs(contributing): add testing guidelines

Add comprehensive testing requirements and examples
for contributors.
```

### Best Practices

- Use imperative mood ("add" not "added")
- First line should be 50 characters or less
- Separate subject from body with blank line
- Wrap body at 72 characters
- Reference issues and PRs in footer

## Adding New Data Sources

DeltaFlow is designed to be extensible. Here's how to add support for a new data source:

### Step-by-Step Guide

#### 1. Create a Reader DoFn Class

Add a new class to `main.py` following this pattern:

```python
class NewSourceReaderDoFn(beam.DoFn):
    """Read data from New Source."""

    def __init__(self, connection_params):
        """Initialize with connection parameters (ValueProviders)."""
        self._connection_params = connection_params

    def setup(self):
        """Set up connection in worker context."""
        # Import libraries here (worker context)
        import new_source_library

        # Resolve ValueProviders
        host = safe_get_param(self._connection_params['host'])

        # Create connection
        self._connection = new_source_library.connect(host=host)

    def process(self, element):
        """Read and yield records."""
        cursor = self._connection.cursor()
        cursor.execute(element)  # element is query

        for row in cursor:
            yield dict(row)

    def teardown(self):
        """Clean up connection."""
        if hasattr(self, '_connection') and self._connection:
            self._connection.close()
```

#### 2. Add Configuration Parameters

Update `CustomPipelineOptions._add_argparse_args()`:

```python
parser.add_value_provider_argument(
    '--new_source_host',
    type=str,
    help='New Source host address'
)
parser.add_value_provider_argument(
    '--new_source_query',
    type=str,
    help='Query to execute on New Source'
)
```

#### 3. Add Schema Detection (for Auto-Schema)

```python
class NewSourceSchemaDetectionDoFn(beam.DoFn):
    """Detect schema from New Source."""

    def setup(self):
        import new_source_library
        # Set up connection

    def process(self, element):
        """Query schema metadata and yield BigQuery schema."""
        # Query information schema or sample data
        # Map to BigQuery types
        schema_fields = [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'data', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
        yield schema_fields
```

#### 4. Integrate into Pipeline

Update `run_pipeline()` function:

```python
if pipeline_options.data_source.get() == 'newsource':
    data_stream = (
        pipeline
        | 'StartNewSource' >> beam.Create(['query'])
        | 'ReadNewSource' >> beam.ParDo(
            NewSourceReaderDoFn(connection_params)
        )
    )
    data_streams.append(data_stream)
```

#### 5. Update metadata.json

Add new parameters to the template metadata:

```json
{
  "name": "new_source_host",
  "label": "New Source Host",
  "helpText": "Host address for New Source connection",
  "paramType": "TEXT"
}
```

#### 6. Update Documentation

- Add usage examples to README.md
- Document parameters in CLAUDE.md
- Add data source to supported sources list

#### 7. Add Tests

Create comprehensive tests for the new source:

```python
def test_new_source_reader():
    """Test New Source reader DoFn."""
    # Mock connection
    # Test data reading
    # Verify output format

def test_new_source_schema_detection():
    """Test schema detection for New Source."""
    # Test schema extraction
    # Verify BigQuery type mapping
```

### Data Source Checklist

- [ ] Reader DoFn implemented with setup/process/teardown
- [ ] Schema detection DoFn implemented (for Auto-Schema)
- [ ] Parameters added to CustomPipelineOptions
- [ ] Integration in run_pipeline()
- [ ] metadata.json updated
- [ ] Type mapping to BigQuery documented
- [ ] Unit tests added
- [ ] Integration tests added
- [ ] Documentation updated
- [ ] Example usage provided

## Documentation Standards

### Code Documentation

1. **Module docstrings**: Describe the module purpose
   ```python
   """
   DeltaFlow: Custom Google Cloud Dataflow Pipeline Template.

   Provides cross-platform data synchronization with Smart Sync
   and Auto-Schema capabilities.
   """
   ```

2. **Class docstrings**: Describe class purpose and usage
   ```python
   class PostgreSQLReaderDoFn(beam.DoFn):
       """
       Read data from PostgreSQL database.

       Uses psycopg2 for direct connection. Implements setup/teardown
       pattern for proper connection management in Dataflow workers.
       """
   ```

3. **Function docstrings**: Use Google style
   ```python
   def sanitize_for_bigquery_json(data):
       """
       Sanitize data for BigQuery JSON compatibility.

       Args:
           data: Dictionary or list to sanitize

       Returns:
           Sanitized data structure compatible with BigQuery JSON

       Raises:
           ValueError: If data contains unsupported types
       """
   ```

4. **Inline comments**: Explain complex logic
   ```python
   # Use 1900-01-01 as start_timestamp to sync all historical data
   # when destination table is empty and sync_all_on_empty_table is True
   ```

### User Documentation

- **README.md**: User-facing documentation, quick start, examples
- **CLAUDE.md**: Developer documentation, architecture, patterns
- **docs/**: Additional guides and tutorials

### Documentation Checklist

- [ ] All public functions have docstrings
- [ ] Complex logic has inline comments
- [ ] README updated for user-facing changes
- [ ] CLAUDE.md updated for developer-facing changes
- [ ] Examples provided for new features
- [ ] Type hints included in function signatures

## Community and Communication

### Getting Help

- **GitHub Issues**: Report bugs or request features
- **GitHub Discussions**: Ask questions, share ideas, get help
- **Pull Requests**: Contribute code and documentation

### Reporting Bugs

Use the GitHub issue tracker with:
- Clear, descriptive title
- Steps to reproduce
- Expected vs. actual behavior
- Environment details (Python version, Beam version, etc.)
- Error messages and logs

### Suggesting Features

Open a GitHub issue with:
- Use case description
- Proposed solution
- Alternative approaches considered
- Willingness to implement

### Community Guidelines

- Be respectful and inclusive
- Provide constructive feedback
- Help others in the community
- Follow our Code of Conduct

---

Thank you for contributing to DeltaFlow! Your efforts help make data synchronization better for everyone.
