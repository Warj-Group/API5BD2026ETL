#!/bin/bash

echo "========================================================"
echo "  WARJ-GROUP - Configurando Ambiente BACKEND (PYTHON)"
echo "========================================================"

if ! command -v npm &> /dev/null || ! command -v python3 &> /dev/null; then
    echo "❌ ERRO: npm ou python3 não encontrados."
    exit 1
fi

echo "📦 [1/6] Instalando Git Hooks (Node)..."
npm init -y
npm install --ignore-scripts --save-dev husky @commitlint/cli @commitlint/config-conventional

echo "⚡ [2/6] Instalando dependências a partir do requirements.txt..."
pip install -r requirements.txt

echo "⚙️  [3/6] Criando pyproject.toml..."
cat << 'EOF' > pyproject.toml
[tool.ruff]
line-length = 88
target-version = "py311"
exclude = [".git", "__pycache__", "venv", ".venv", "node_modules"]

[tool.ruff.lint]
select = ["E", "F", "I"] # E/F = Erros de Sintaxe, I = Organização de Imports

[tool.mypy]
python_version = "3.11"
ignore_missing_imports = true
EOF

echo "📜 [4/6] Criando commitlint.config.js..."
cat << 'EOF' > commitlint.config.js
module.exports = {
  parserPreset: {
    parserOpts: {
      headerPattern: /^(feat|fix|doc|style|refactor|test|chore|ci)\/(WARJ-\d+|main|sprint-\d+): (.+)$/,
      headerCorrespondence: ['type', 'scope', 'subject']
    }
  },
  rules: {
    'type-empty': [2, 'never'],
    'subject-empty': [2, 'never'],
    'type-enum': [2, 'always', ['feat', 'fix', 'doc', 'style', 'refactor', 'test', 'chore', 'ci']]
  }
};
EOF

echo "🐶 [5/6] Inicializando Husky..."
npx husky init

echo "🛠️ [6/6] Configurando Hooks do Husky..."

cat << 'EOF' > .husky/commit-msg
#!/bin/bash
npx commitlint --edit "$1" || {
  echo -e "\n\033[0;31m❌ ERRO: Mensagem de commit fora do padrão Warj-Group!\033[0m"
  echo "PADRÃO: {tipo}/{id_yt}: Descrição"
  exit 1
}
EOF

cat << 'EOF' > .husky/pre-commit
#!/bin/bash
BRANCH=$(git rev-parse --abbrev-ref HEAD)
REGEX="^(main|sprint-[0-9]+)$|^(feature|hotfix|release)\/WARJ-[0-9]+-.+$"

if [[ ! $BRANCH =~ $REGEX ]]; then
  echo -e "\n\033[0;31m❌ ERRO: Nome da branch fora do padrão Warj-Group!\033[0m"
  exit 1
fi

echo "⚡ Validando código Python com Ruff..."
ruff check .
ruff format --check .

echo "🔍 Checando tipagem com Mypy..."
mypy .
EOF

chmod +x .husky/commit-msg
chmod +x .husky/pre-commit

echo "========================================================"
echo "  ✅ SUCESSO! Ambiente backend configurado."
echo "========================================================"