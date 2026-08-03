import { spawn } from 'node:child_process';
import { resolve } from 'node:path';

const WORKER_PATH = resolve(__dirname, 'worker.js');

export interface WorkerCommand {
  [key: string]: unknown;
}

export interface WorkerResult {
  ok: boolean;
  error?: string;
  code?: string;
  killed?: boolean;
  signal?: string;
  [key: string]: unknown;
}

export function runWorker(cmd: WorkerCommand): Promise<WorkerResult> {
  return new Promise((resolveP, reject) => {
    const child = spawn(process.execPath, [WORKER_PATH], {
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (d) => {
      stdout += d.toString();
    });
    child.stderr.on('data', (d) => {
      stderr += d.toString();
    });

    child.on('error', (err) => {
      reject(err);
    });

    child.on('exit', (code, signal) => {
      if (signal === 'SIGKILL' || code === null) {
        if (stdout.trim()) {
          try {
            resolveP(JSON.parse(stdout.trim()));
            return;
          } catch {
            // fall through
          }
        }
        resolveP({ ok: true, killed: true, signal: signal ?? undefined });
        return;
      }
      if (stdout.trim()) {
        try {
          resolveP(JSON.parse(stdout.trim()));
          return;
        } catch {
          // fall through
        }
      }
      if (code !== 0) {
        if (!stderr.trim() && !stdout.trim()) {
          resolveP({ ok: true, killed: true, exitCode: code });
          return;
        }
        reject(
          new Error(
            `Worker exited with code ${code}. stderr: ${stderr} stdout: ${stdout}`,
          ),
        );
        return;
      }
      reject(new Error('Worker produced no output. stderr: ' + stderr));
    });

    child.stdin.write(JSON.stringify(cmd));
    child.stdin.end();
  });
}
