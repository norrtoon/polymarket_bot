#!/usr/bin/env python3
"""
Статическая проверка перед деплоем.

Ловит класс ошибок, который уже дважды доезжал до боевого режима:
обращение к переменной, которой нет в области видимости (NameError в
рантайме). Такие баги не видны при импорте модуля и проявляются только
когда выполнится конкретная ветка — то есть уже на реальных деньгах.

Запуск:  python tools/check_code.py
Код возврата 1 — найдены проблемы, деплоить нельзя.
"""
import ast
import builtins
import os
import sys

SKIP_DIRS = {"venv", "__pycache__", ".git", "tests", "tools"}


def module_scope(tree):
    scope = set(dir(builtins))

    def walk(body):
        for n in body:
            if isinstance(n, ast.Import):
                for a in n.names:
                    scope.add((a.asname or a.name).split(".")[0])
            elif isinstance(n, ast.ImportFrom):
                for a in n.names:
                    scope.add(a.asname or a.name)
            elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef,
                                ast.ClassDef)):
                scope.add(n.name)
            elif isinstance(n, ast.Assign):
                for t in n.targets:
                    if isinstance(t, ast.Name):
                        scope.add(t.id)
            elif isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name):
                scope.add(n.target.id)
            elif isinstance(n, ast.Try):
                walk(n.body)
                for h in n.handlers:
                    walk(h.body)
                walk(n.orelse)
                walk(n.finalbody)
            elif isinstance(n, ast.If):
                walk(n.body)
                walk(n.orelse)

    walk(tree.body)
    return scope


def bound_names(node):
    names = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store):
            names.add(n.id)
        elif isinstance(n, ast.ExceptHandler) and n.name:
            names.add(n.name)
        elif isinstance(n, ast.Import):
            for a in n.names:
                names.add((a.asname or a.name).split(".")[0])
        elif isinstance(n, ast.ImportFrom):
            for a in n.names:
                names.add(a.asname or a.name)
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(n.name)
            for a in (n.args.args + n.args.kwonlyargs + n.args.posonlyargs):
                names.add(a.arg)
            if n.args.vararg:
                names.add(n.args.vararg.arg)
            if n.args.kwarg:
                names.add(n.args.kwarg.arg)
        elif isinstance(n, ast.Lambda):
            for a in (n.args.args + n.args.kwonlyargs):
                names.add(a.arg)
        elif isinstance(n, ast.withitem) and n.optional_vars:
            for t in ast.walk(n.optional_vars):
                if isinstance(t, ast.Name):
                    names.add(t.id)
        elif isinstance(n, ast.comprehension):
            for t in ast.walk(n.target):
                if isinstance(t, ast.Name):
                    names.add(t.id)
    return names


def check_file(path):
    problems = []
    try:
        tree = ast.parse(open(path, encoding="utf-8").read())
    except SyntaxError as e:
        return [("СИНТАКСИС", str(e), e.lineno or 0)]

    scope = module_scope(tree)

    def check_fn(fn, outer):
        visible = outer | bound_names(fn)

        class Body(ast.NodeVisitor):
            def visit_FunctionDef(self, n):
                if n is fn:
                    for st in n.body:
                        self.visit(st)

            visit_AsyncFunctionDef = visit_FunctionDef

            def visit_Name(self, n):
                if isinstance(n.ctx, ast.Load) and n.id not in visible:
                    if n.id not in ("self", "cls"):
                        problems.append((fn.name, n.id, n.lineno))

        Body().visit(fn)
        for st in fn.body:
            for ch in ast.walk(st):
                if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    check_fn(ch, visible)

    def walk_defs(body):
        for n in body:
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                check_fn(n, scope)
            elif isinstance(n, ast.ClassDef):
                walk_defs(n.body)

    walk_defs(tree.body)
    return problems


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    found = 0
    for dirpath, dirnames, filenames in os.walk("."):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if not name.endswith(".py"):
                continue
            path = os.path.join(dirpath, name)
            issues = check_file(path)
            if issues:
                found += len(issues)
                print(f"\n{path}:")
                for fn, var, line in sorted(set(issues), key=lambda x: x[2]):
                    print(f"  строка {line}: {fn}() использует {var!r}")

    if found:
        print(f"\n❌ Найдено проблем: {found}. Деплой остановлен.")
        return 1
    print("✅ Статическая проверка пройдена")
    return 0


if __name__ == "__main__":
    sys.exit(main())