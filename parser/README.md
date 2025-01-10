# YQL ANTLR Parser Generator

This project provides code generation of YQL and SQL parsers using ANTLR4.

---

## Makefile Targets

### General Targets

- **`all`**  
  Generates both YQL and SQL parsers.  
  ```bash
  make all
  ```

- **`build-image`**  
  Builds the Docker image required for parser generation.  
  ```bash
  make build-image
  ```

- **`clean`**  
  Cleans all generated files for both YQL and SQL.  
  ```bash
  make clean
  ```

- **`regenerate`**  
  Cleans and regenerates all parser files (YQL and SQL).  
  ```bash
  make regenerate
  ```
### YQL Parser Targets

- **`yql`**  
  Generates the YQL parser files.  
  ```bash
  make yql
  ```

- **`clean_yql`**  
  Removes all generated YQL parser files (`*.go`, `*.interp`, `*.tokens`).  
  ```bash
  make clean_yql
  ```

- **`regenerate_yql`**  
  Cleans and regenerates the YQL parser files.  
  ```bash
  make regenerate_yql
  ```

### SQL Parser Targets

- **`sql`**  
  Generates the SQL parser files.  
  ```bash
  make sql
  ```

- **`clean_sql`**  
  Removes all generated SQL parser files (`*.go`, `*.interp`, `*.tokens`).  
  ```bash
  make clean_sql
  ```

- **`regenerate_sql`**  
  Cleans and regenerates the SQL parser files.  
  ```bash
  make regenerate_sql
  ```

---

## Project Structure

- **`yql/`**: Directory for YQL parser files.
- **`sql/`**: Directory for SQL parser files.
- **`Dockerfile`**: Dockerfile for the parser generation environment.
- **`Makefile`**: Contains all automation commands.