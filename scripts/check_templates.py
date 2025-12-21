#!/usr/bin/env python3
import sys
import os
from jinja2 import Environment, FileSystemLoader, TemplateSyntaxError, TemplateError, select_autoescape


def main():
    base = os.path.join(os.path.dirname(__file__), "..")
    templates_dir = os.path.join(base, "templates")
    if not os.path.isdir(templates_dir):
        print("templates directory not found:", templates_dir)
        return 2

    env = Environment(loader=FileSystemLoader(templates_dir), autoescape=select_autoescape(["html", "xml"]))

    errors = []
    templates = [t for t in env.list_templates() if t.startswith("applicant/")]
    if not templates:
        print("No applicant templates found to check.")
        return 0

    print(f"Checking {len(templates)} applicant templates for Jinja syntax...")
    for t in sorted(templates):
        try:
            src, filename, uptodate = env.loader.get_source(env, t)
            # parse only (compilation) to avoid rendering issues
            env.parse(src)
            print(f"OK: {t}")
        except TemplateSyntaxError as e:
            msg = f"SYNTAX ERROR in {t}: {e.message} (line {e.lineno})"
            print(msg)
            errors.append(msg)
        except TemplateError as e:
            msg = f"TEMPLATE ERROR in {t}: {e}"
            print(msg)
            errors.append(msg)
        except Exception as e:
            msg = f"ERROR checking {t}: {e}"
            print(msg)
            errors.append(msg)

    if errors:
        print("\nSummary: Found", len(errors), "issue(s).")
        return 1

    print("\nAll applicant templates parsed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
