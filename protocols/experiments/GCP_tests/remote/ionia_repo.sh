# Shared helpers: install deploy SSH key from setup bundle and clone/update IONIA from GitHub.
# Sourced by remote/setup_paxos_vr.sh and remote/setup_dyna.sh

IONIA_GIT_URL="${IONIA_GIT_URL:-git@github.com:ntu2323/IONIA.git}"
IONIA_DIR="${IONIA_DIR:-${HOME}/Ionia}"
USE_GIT="${USE_GIT:-1}"

git_ssh() {
  export GIT_SSH_COMMAND="ssh -i ${HOME}/.ssh/id_ed25519 -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
}

install_ssh_from_bundle() {
  mkdir -p ~/.ssh ~/bench
  chmod 700 ~/.ssh
  cp "${SETUP_BUNDLE}/id_ed25519" ~/.ssh/id_ed25519
  chmod 600 ~/.ssh/id_ed25519
  touch ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys
  pub="$(tr -d '\r\n' < "${SETUP_BUNDLE}/ssh_public_line")"
  grep -qxF "${pub}" ~/.ssh/authorized_keys || echo "${pub}" >> ~/.ssh/authorized_keys
}

setup_github_known_hosts() {
  mkdir -p ~/.ssh
  chmod 700 ~/.ssh
  touch ~/.ssh/known_hosts
  chmod 600 ~/.ssh/known_hosts
  ssh-keyscan -t ed25519,rsa github.com >> ~/.ssh/known_hosts 2>/dev/null || true
}

require_git() {
  if ! command -v git >/dev/null 2>&1; then
    echo "git is not installed (apt install git)" >&2
    exit 1
  fi
  git --version
}

ensure_ionia_repo() {
  if [[ "${USE_GIT}" != "1" ]]; then
    if [[ -d "${IONIA_DIR}" ]]; then
      echo "Using existing ${IONIA_DIR} (rsync/local, USE_GIT=0)"
      return 0
    fi
    echo "Missing ${IONIA_DIR} — rsync local tree or set USE_GIT=1" >&2
    exit 1
  fi

  require_git
  setup_github_known_hosts
  git_ssh

  if [[ -d "${IONIA_DIR}/.git" ]]; then
    echo "Updating ${IONIA_DIR} from ${IONIA_GIT_URL}..."
    cd "${IONIA_DIR}"
    git fetch origin
    ref="${IONIA_GIT_REF:-}"
    if [[ -n "${ref}" ]]; then
      git checkout "${ref}"
      git pull --ff-only origin "${ref}" || true
    else
      git pull --ff-only || git pull --ff-only origin main || git pull --ff-only origin master || true
    fi
    git submodule update --init --recursive
    cd "${HOME}"
    return 0
  fi

  if [[ -d "${IONIA_DIR}" ]]; then
    echo "Removing non-git ${IONIA_DIR} before clone..."
    rm -rf "${IONIA_DIR}"
  fi

  echo "Cloning ${IONIA_GIT_URL} -> ${IONIA_DIR}..."
  git clone "${IONIA_GIT_URL}" "${IONIA_DIR}"
  cd "${IONIA_DIR}"
  ref="${IONIA_GIT_REF:-}"
  if [[ -n "${ref}" ]]; then
    git checkout "${ref}"
  fi
  git submodule update --init --recursive
  cd "${HOME}"
}
