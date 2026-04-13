#!/usr/bin/env bash

set -euo pipefail

COMMIT_MSG="${1:-}"
BRANCH="${2:-}"
DATA_HORA="$(date '+%Y-%m-%d %H:%M:%S')"

if [[ -z "$COMMIT_MSG" ]]; then
  echo "Uso:"
  echo "  ./publicar_git.sh \"mensagem do commit\" [branch]"
  echo
  echo "Exemplo:"
  echo "  ./publicar_git.sh \"Ajusta pipeline de carga do SINP\" main"
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "Erro: git não encontrado no ambiente."
  exit 1
fi

if [[ ! -d ".git" ]]; then
  echo "Erro: este diretório não é um repositório Git."
  exit 1
fi

REPO_DIR="$(basename "$(pwd)")"

if [[ -z "$BRANCH" ]]; then
  BRANCH="$(git rev-parse --abbrev-ref HEAD)"
fi

REMOTE_URL="$(git remote get-url origin 2>/dev/null || true)"

if [[ -z "$REMOTE_URL" ]]; then
  echo "Erro: remoto 'origin' não configurado."
  echo "Configure antes com:"
  echo "  git remote add origin <URL_DO_REPOSITORIO>"
  exit 1
fi

echo "=================================================="
echo "Repositório : $REPO_DIR"
echo "Branch      : $BRANCH"
echo "Remoto      : $REMOTE_URL"
echo "Data/Hora   : $DATA_HORA"
echo "=================================================="
echo

echo "[1/6] Status atual"
git status --short || true
echo

echo "[2/6] Adicionando arquivos"
git add .
echo

if git diff --cached --quiet; then
  echo "Nenhuma alteração para commitar."
  exit 0
fi

echo "[3/6] Criando commit"
git commit -m "$COMMIT_MSG"
echo

echo "[4/6] Enviando para o remoto"
git push -u origin "$BRANCH"
echo

echo "[5/6] Coletando evidências"
COMMIT_HASH="$(git rev-parse HEAD)"
COMMIT_CURTO="$(git rev-parse --short HEAD)"
AUTOR="$(git log -1 --pretty=format:'%an <%ae>')"
DATA_COMMIT="$(git log -1 --pretty=format:'%ad' --date=iso)"
ASSUNTO="$(git log -1 --pretty=format:'%s')"

ARQUIVO_EVIDENCIA="evidencia_git_${REPO_DIR}_$(date '+%Y%m%d_%H%M%S').txt"

{
  echo "EVIDÊNCIA DE PUBLICAÇÃO GIT"
  echo "=================================================="
  echo "Repositório       : $REPO_DIR"
  echo "Branch            : $BRANCH"
  echo "Remoto            : $REMOTE_URL"
  echo "Commit hash       : $COMMIT_HASH"
  echo "Commit curto      : $COMMIT_CURTO"
  echo "Mensagem commit   : $ASSUNTO"
  echo "Autor             : $AUTOR"
  echo "Data commit       : $DATA_COMMIT"
  echo "Data evidência    : $DATA_HORA"
  echo "=================================================="
  echo
  echo "ARQUIVOS ALTERADOS NO ÚLTIMO COMMIT"
  echo "--------------------------------------------------"
  git show --stat --oneline --name-only --no-patch "$COMMIT_HASH"
  echo
  git diff-tree --no-commit-id --name-only -r "$COMMIT_HASH"
  echo
  echo "DETALHE RESUMIDO DO COMMIT"
  echo "--------------------------------------------------"
  git log -1 --decorate --stat
} > "$ARQUIVO_EVIDENCIA"

echo "[6/6] Evidência gerada"
echo "Arquivo: $ARQUIVO_EVIDENCIA"
echo

echo "=================================================="
echo "TEXTO PARA ENVIAR AO GERENTE"
echo "=================================================="
echo "Atualização publicada no repositório Git."
echo "Branch: $BRANCH"
echo "Commit: $COMMIT_CURTO"
echo "Descrição: $ASSUNTO"
echo "Status: disponível para validação."
echo "Evidência anexada: $ARQUIVO_EVIDENCIA"
echo "=================================================="
