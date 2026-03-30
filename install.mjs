#!/usr/bin/env node
/**
 * cortextOS cross-platform installer
 *
 * Mac/Linux:   curl -fsSL https://get.cortextos.dev/install.mjs | node
 * Windows:     node -e "$(irm https://get.cortextos.dev/install.mjs)"
 * Local test:  node install.mjs
 */

import { execSync, spawnSync } from 'child_process';
import { existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import { homedir, platform } from 'os';

const REPO_URL = process.env.CORTEXTOS_REPO || 'https://github.com/grandamenium/cortextos.git';
const INSTALL_DIR = process.env.CORTEXTOS_DIR || join(homedir(), 'cortextos');
const IS_WINDOWS = platform() === 'win32';

// ANSI colors (work on modern Windows Terminal, macOS Terminal, Linux)
const R = '\x1b[0m';
const B = '\x1b[34m';
const G = '\x1b[32m';
const Y = '\x1b[33m';
const RED = '\x1b[31m';
const BOLD = '\x1b[1m';

const log  = (msg) => console.log(`${B}==>${R} ${msg}`);
const ok   = (msg) => console.log(`${G}  ✓${R} ${msg}`);
const warn = (msg) => console.log(`${Y}  !${R} ${msg}`);
const fail = (msg) => { console.error(`${RED}  ✗${R} ${msg}`); process.exit(1); };

function run(cmd, opts = {}) {
  return execSync(cmd, { encoding: 'utf8', stdio: 'pipe', ...opts }).trim();
}

function runVisible(cmd, opts = {}) {
  execSync(cmd, { stdio: 'inherit', ...opts });
}

function commandExists(cmd) {
  try {
    const which = IS_WINDOWS ? 'where' : 'which';
    run(`${which} ${cmd}`);
    return true;
  } catch {
    return false;
  }
}

console.log('');
console.log(`${BOLD}cortextOS installer${R}`);
console.log('Persistent 24/7 Claude Code agents with Telegram control');
console.log('');

// --- Prerequisite checks ---

log('Checking prerequisites...');

// Node.js 20+
const nodeVersion = run('node --version').replace('v', '');
const nodeMajor = parseInt(nodeVersion.split('.')[0], 10);
if (nodeMajor < 20) {
  fail(`Node.js v${nodeVersion} is too old. v20 or later required. Install from https://nodejs.org`);
}
ok(`Node.js v${nodeVersion}`);

// npm
try {
  const npmVersion = run('npm --version');
  ok(`npm ${npmVersion}`);
} catch {
  fail('npm is not installed');
}

// Claude Code
if (commandExists('claude')) {
  try {
    const claudeVersion = run('claude --version').split('\n')[0];
    ok(`Claude Code ${claudeVersion}`);
  } catch {
    ok('Claude Code (installed)');
  }
} else {
  warn('Claude Code is not installed. Install it after setup:');
  warn('  npm install -g @anthropic-ai/claude-code');
}

// Claude Code auth
try {
  run('claude --version');
  ok('Claude Code authenticated (using stored login)');
} catch {
  warn('Run "claude login" to authenticate Claude Code before starting agents.');
}

console.log('');

// --- Clone or update ---

if (existsSync(INSTALL_DIR)) {
  warn(`Directory ${INSTALL_DIR} already exists`);
  if (existsSync(join(INSTALL_DIR, '.git'))) {
    log('Pulling latest changes...');
    try {
      runVisible('git pull --ff-only', { cwd: INSTALL_DIR });
    } catch {
      warn('Could not pull — continuing with existing version');
    }
  } else {
    fail(`${INSTALL_DIR} exists but is not a git repo. Remove it or set CORTEXTOS_DIR to a different path.`);
  }
} else {
  log(`Cloning cortextOS to ${INSTALL_DIR}...`);
  runVisible(`git clone ${REPO_URL} ${JSON.stringify(INSTALL_DIR)}`);
  ok('Cloned');
}

// --- Install dependencies ---

log('Installing dependencies...');
runVisible('npm install --silent', { cwd: INSTALL_DIR });
ok('Dependencies installed');

// --- Build ---

log('Building...');
runVisible('npm run build --silent', { cwd: INSTALL_DIR });
ok('Build complete');

// --- Link CLI globally ---

log('Linking cortextos CLI...');
try {
  runVisible('npm link --silent', { cwd: INSTALL_DIR });
} catch {
  try {
    runVisible('npm install -g . --silent', { cwd: INSTALL_DIR });
  } catch {
    warn('Could not install globally. Run manually: cd ' + INSTALL_DIR + ' && npm install -g .');
  }
}

if (commandExists('cortextos')) {
  ok('cortextos CLI available');
} else {
  warn('cortextos not in PATH yet. You may need to restart your terminal.');
}

// --- PM2 ---

if (!commandExists('pm2')) {
  log('Installing PM2 (process manager)...');
  runVisible('npm install -g pm2 --silent');
  ok('PM2 installed');
} else {
  ok(`PM2 ${run('pm2 --version')}`);
}

// --- Done ---

console.log('');
console.log(`${G}${BOLD}cortextOS installed successfully!${R}`);
console.log('');
console.log(`${BOLD}Next steps:${R}`);
console.log('');
console.log(`  1. Open ${BOLD}${INSTALL_DIR}${R} in Claude Code:`);
if (IS_WINDOWS) {
  console.log(`     ${Y}claude "${INSTALL_DIR}"${R}`);
} else {
  console.log(`     ${Y}claude ${INSTALL_DIR}${R}`);
}
console.log('');
console.log(`  2. In Claude Code, run:`);
console.log(`     ${Y}/onboarding${R}`);
console.log('');
console.log('  That\'s it. The /onboarding command walks you through everything:');
console.log('  org setup, agent creation, Telegram bots, dashboard, and more.');
console.log('');
