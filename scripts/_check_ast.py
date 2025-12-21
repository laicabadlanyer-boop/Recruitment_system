import ast, sys
p = r'c:\xampp\htdocs\Recruitment System\app.py'
with open(p, 'r', encoding='utf-8') as f:
    s = f.read()
try:
    ast.parse(s)
    print('AST OK')
except Exception as e:
    print('AST ERROR:', type(e).__name__, e)
    sys.exit(1)
