#!/bin/sh
':' //; if command -v node >/dev/null 2>&1; then exec node "$0" "$@"; fi; exec bun "$0" "$@"

import { randomBytes } from 'node:crypto';

const newKey = randomBytes(32).toString("hex");
console.log(`\nTHIS IS YOUR NEW ENCRYPTION KEY:\n\n\x1b[31m '${newKey}' \x1b[0m\n\n`);
