@echo off
setlocal enabledelayedexpansion

echo ========================================================
echo   WARJ-GROUP - Configurando Ambiente ETL (PYTHON)
echo ========================================================

:: Verificar dependencias
where npm >nul 2>nul || (echo [ERRO] npm nao encontrado. & pause & exit /b 1)
where python >nul 2>nul || (echo [ERRO] python nao encontrado. & pause & exit /b 1)

echo [1/6] Instalando dependencias de Git (Husky + Commitlint)...
call npm init -y
call npm install --ignore-scripts --save-dev husky @commitlint/cli @commitlint/config-conventional

echo [2/6] Instalando dependencias a partir do requirements.txt...
call pip install -r requirements.txt

echo [3/6] Criando arquivo central de configuracoes (pyproject.toml)...
(
echo [tool.ruff]
echo line-length = 88
echo target-version = "py311"
echo exclude = [".git", "__pycache__", "venv", ".venv", "node_modules"]
echo.
echo [tool.ruff.lint]
echo select = ["E", "F", "I"]
echo.
echo [tool.mypy]
echo python_version = "3.11"
echo ignore_missing_imports = true
) > pyproject.toml

echo [4/6] Criando commitlint.config.js...
(
echo module.exports = {
echo   parserPreset: {
echo     parserOpts: {
echo       headerPattern: /^^(feat^|fix^|doc^|style^|refactor^|test^|chore^|ci^)\/(WARJ-\d+^|main^|sprint-\d+^): (.+^)$/,
echo       headerCorrespondence: ['type', 'scope', 'subject']
echo     }
echo   },
echo   rules: {
echo     'type-empty': [2, 'never'],
echo     'subject-empty': [2, 'never'],
echo     'type-enum': [2, 'always', ['feat', 'fix', 'doc', 'style', 'refactor', 'test', 'chore', 'ci']]
echo   }
echo };
) > commitlint.config.js

echo [5/6] Inicializando Husky...
call npm exec husky init

echo [6/6] Configurando Hooks de seguranca...

:: Hook de Mensagem
(
echo #!/bin/bash
echo npx commitlint --edit "$1" ^|^| {
echo   echo -e "\n\033[0;31mXXXX ERRO: Mensagem de commit fora do padrao Warj-Group!\033[0m"
echo   echo "PADRAO: {tipo}/{id_yt}: Descricao"
echo   exit 1
echo }
) > .husky\commit-msg

:: Hook de Branch + Ruff + Mypy
(
echo #!/bin/bash
echo BRANCH=$(git rev-parse --abbrev-ref HEAD^)
echo REGEX="^^(main^|sprint-[0-9]+^)$^|^^(feature^|hotfix^|release^)\/WARJ-[0-9]+-.+$"
echo if [[ ! $BRANCH =~ $REGEX ]]; then
echo   echo -e "\n\033[0;31mXXXX ERRO: Nome da branch fora do padrao Warj-Group!\033[0m"
echo   exit 1
echo fi
echo echo "⚡ Validando codigo Python com Ruff..."
echo ruff check .
echo ruff format --check .
echo echo "🔍 Checando tipagem com Mypy..."
echo mypy .
) > .husky\pre-commit

echo ========================================================
echo   SUCESSO! Ambiente ETL Configurado.
echo ========================================================
pause