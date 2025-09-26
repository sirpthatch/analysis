# Analysis Project

This repository contains multiple analysis projects, each in its own subdirectory with isolated virtual environments.

## Project Structure

```
analysis/
├── README.md
└── projects/
    └── public_parks/
        ├── venv/           # Virtual environment
        ├── requirements.txt # Python dependencies
        └── activate.sh     # Environment activation script
```

## Getting Started with a Project

### Public Parks Project

1. Navigate to the project directory:
   ```bash
   cd projects/public_parks
   ```

2. Activate the virtual environment:
   ```bash
   source activate.sh
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Adding New Projects

To create a new analysis project:

1. Create a new directory under `projects/`:
   ```bash
   mkdir projects/your_project_name
   cd projects/your_project_name
   ```

2. Create a virtual environment:
   ```bash
   python3 -m venv venv
   ```

3. Create requirements.txt and activation script following the public_parks example.

## Best Practices

- Keep each project isolated in its own virtual environment
- Update requirements.txt when adding new dependencies
- Use descriptive project names
- Document project-specific setup in individual README files within each project directory
