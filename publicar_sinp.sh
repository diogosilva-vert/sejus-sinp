#!/usr/bin/env bash

set -euo pipefail

COMMIT_MSG="${1:-Atualiza projeto SINP}"
BRANCH="${2:-$(git rev-parse --abbrev-ref HEAD)}"
REMOTE_EXPECTED="https://github.com/vert-brasil/sinp.git"
REPO_DIR="$(basename "$(pwd)")"
DATA_HORA="$(date '+%Y-%m-%d %H:%M:%S')"

echo "=================================================="
echo "Publicação Git - SINP"
echo "Repositório : $REPO_DIR"
echo "Branch      : $BRANCH"
echo "Data/Hora   : $DATA_HORA"
echo "=================================================="

if ! command -v git >/dev/null 2>&1; then
  echo "ERRO: git não encontrado."
  exit 1
fi

if [[ ! -d ".git" ]]; then
  echo "ERRO: diretório atual não é um repositório git."
  exit 1
fi

REMOTE_URL="$(git remote get-url origin 2>/dev/null || true)"
if [[ -z "$REMOTE_URL" ]]; then
  echo "ERRO: remoto origin não configurado."
  exit 1
fi

if [[ "$REMOTE_URL" != "$REMOTE_EXPECTED" ]]; then
  echo "ERRO: remoto origin diferente do esperado."
  echo "Atual   : $REMOTE_URL"
  echo "Esperado: $REMOTE_EXPECTED"
  exit 1
fi

echo
echo "[1/7] Status atual"
git status --short || true

echo
echo "[2/7] Adicionando alterações"
git add .

if git diff --cached --quiet; then
  echo
  echo "Nenhuma alteração encontrada para commit."
  exit 0
fi

echo
echo "[3/7] Criando commit"
git commit -m "$COMMIT_MSG"

echo
echo "[4/7] Enviando para GitHub"
git push -u origin "$BRANCH"

echo
echo "[5/7] Coletando informações"
COMMIT_HASH="$(git rev-parse HEAD)"
COMMIT_CURTO="$(git rev-parse --short HEAD)"
AUTOR="$(git log -1 --pretty=format:'%an <%ae>')"
DATA_COMMIT="$(git log -1 --pretty=format:'%ad' --date=iso)"
ASSUNTO="$(git log -1 --pretty=format:'%s')"

ARQUIVO_EVIDENCIA="evidencia_git_sinp_$(date '+%Y%m%d_%H%M%S').txt"

{
  echo "EVIDENCIA DE ATUALIZACAO GIT"
  echo "=================================================="
  echo "Repositorio     : $REPO_DIR"
  echo "Remote          : $REMOTE_URL"
  echo "Branch          : $BRANCH"
  echo "Commit hash     : $COMMIT_HASH"
  echo "Commit curto    : $COMMIT_CURTO"
  echo "Mensagem        : $ASSUNTO"
  echo "Autor           : $AUTOR"
  echo "Data commit     : $DATA_COMMIT"
  echo "Gerado em       : $DATA_HORA"
  echo "=================================================="
  echo
  echo "ULTIMO COMMIT"
  echo "--------------------------------------------------"
  git log -1 --decorate --stat
  echo
  echo "ARQUIVOS ALTERADOS"
  echo "--------------------------------------------------"
  git diff-tree --no-commit-id --name-only -r "$COMMIT_HASH"
  echo
  echo "STATUS FINAL"
  echo "--------------------------------------------------"
  git status
} > "$ARQUIVO_EVIDENCIA"

echo
echo "[6/7] Evidência gerada"
echo "Arquivo: $ARQUIVO_EVIDENCIA"

echo
echo "[7/7] Texto pronto para envio"
echo "=================================================="
echo "Atualização publicada no repositório GitHub do projeto SINP."
echo "Branch: $BRANCH"
echo "Commit: $COMMIT_CURTO"
echo "Descrição: $ASSUNTO"
echo "Repositório: $REMOTE_URL"
echo "Status: disponível para validação."
echo "Evidência: $ARQUIVO_EVIDENCIA"
echo "=================================================="
