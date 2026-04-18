import "dotenv/config";
import express from "express";
import { createClient } from "@supabase/supabase-js";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const app = express();
app.use(express.json({ limit: "10mb" }));

type CompressionLevel = "light" | "balanced" | "strong";

function getConfig() {
  return {
    port: Number(process.env.PORT || 8080),
    workerSecret: process.env.PDF_COMPRESSOR_WORKER_SECRET || "",
    supabaseUrl: process.env.NEXT_PUBLIC_SUPABASE_URL || "",
    serviceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY || "",
    qpdfBin: process.env.QPDF_BIN || "qpdf",
  };
}

function createSupabaseAdmin() {
  const { supabaseUrl, serviceRoleKey } = getConfig();

  if (!supabaseUrl) {
    throw new Error("Missing NEXT_PUBLIC_SUPABASE_URL");
  }

  if (!serviceRoleKey) {
    throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
  }

  return createClient(supabaseUrl, serviceRoleKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
      detectSessionInUrl: false,
    },
  });
}

function sanitizeFilename(filename: string) {
  const cleaned = filename
    .replace(/[^a-zA-Z0-9._-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^\.+/, "")
    .slice(0, 120);

  return cleaned || "document.pdf";
}

async function updateJob(
  jobId: string,
  payload: Record<string, unknown>
) {
  const supabase = createSupabaseAdmin();
  await supabase.from("compression_jobs").update(payload).eq("id", jobId);
}

function buildQpdfArgs(
  level: CompressionLevel,
  inputPath: string,
  outputPath: string
) {
  const baseArgs = [
    "--stream-data=compress",
    "--recompress-flate",
    "--object-streams=generate",
  ];

  if (level === "light") {
    return [
      ...baseArgs,
      "--compression-level=6",
      inputPath,
      outputPath,
    ];
  }

  if (level === "balanced") {
    return [
      ...baseArgs,
      "--compression-level=9",
      "--linearize",
      inputPath,
      outputPath,
    ];
  }

  return [
    ...baseArgs,
    "--compression-level=9",
    "--linearize",
    "--optimize-images",
    "--jpeg-quality=60",
    inputPath,
    outputPath,
  ];
}

app.get("/", (_req, res) => {
  res.send("PDF Compressor Worker is running");
});

app.get("/health", (_req, res) => {
  const config = getConfig();

  res.json({
    ok: true,
    env: {
      PORT: !!process.env.PORT,
      NEXT_PUBLIC_SUPABASE_URL: !!config.supabaseUrl,
      SUPABASE_SERVICE_ROLE_KEY: !!config.serviceRoleKey,
      PDF_COMPRESSOR_WORKER_SECRET: !!config.workerSecret,
      QPDF_BIN: config.qpdfBin,
    },
  });
});

app.post("/jobs/compress", async (req, res) => {
  let tempDir = "";
  const jobId = req.body?.jobId as string | undefined;

  try {
    const {
      workerSecret,
      qpdfBin,
    } = getConfig();

    if (!workerSecret) {
      return res.status(500).json({
        error: "Missing PDF_COMPRESSOR_WORKER_SECRET",
      });
    }

    const incomingSecret = req.header("x-worker-secret");

    if (!incomingSecret || incomingSecret !== workerSecret) {
      return res.status(401).json({ error: "Unauthorized worker request" });
    }

    const {
      inputBucket,
      inputPath,
      outputBucket,
      outputPath,
      compressionLevel,
      originalFilename,
    } = req.body as {
      jobId?: string;
      inputBucket?: string;
      inputPath?: string;
      outputBucket?: string;
      outputPath?: string;
      compressionLevel?: CompressionLevel;
      originalFilename?: string;
    };

    if (
      !jobId ||
      !inputBucket ||
      !inputPath ||
      !outputBucket ||
      !outputPath ||
      !compressionLevel ||
      !originalFilename
    ) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    if (!["light", "balanced", "strong"].includes(compressionLevel)) {
      return res.status(400).json({ error: "Invalid compression level" });
    }

    const supabase = createSupabaseAdmin();

    await updateJob(jobId, {
      status: "processing",
      progress: 10,
      worker_started_at: new Date().toISOString(),
      error_message: null,
    });

    const { data: inputFile, error: downloadError } = await supabase.storage
      .from(inputBucket)
      .download(inputPath);

    if (downloadError || !inputFile) {
      await updateJob(jobId, {
        status: "failed",
        progress: 0,
        error_message:
          downloadError?.message || "Failed to download input file",
        worker_finished_at: new Date().toISOString(),
      });

      return res.status(500).json({
        error: downloadError?.message || "Failed to download input file",
      });
    }

    await updateJob(jobId, {
      progress: 25,
    });

    const originalArrayBuffer = await inputFile.arrayBuffer();
    const originalBuffer = Buffer.from(originalArrayBuffer);
    const safeFilename = sanitizeFilename(originalFilename);
    const outputFilename = `compressed-${safeFilename}`;

    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "pdf-compress-"));

    const localInputPath = path.join(tempDir, safeFilename);
    const localOutputPath = path.join(tempDir, outputFilename);

    await fs.writeFile(localInputPath, originalBuffer);

    await updateJob(jobId, {
      progress: 40,
    });

    const qpdfArgs = buildQpdfArgs(
      compressionLevel,
      localInputPath,
      localOutputPath
    );

    try {
      await execFileAsync(qpdfBin, qpdfArgs);
    } catch (qpdfError) {
      const message =
        qpdfError instanceof Error
          ? qpdfError.message
          : "qpdf compression failed";

      await updateJob(jobId, {
        status: "failed",
        progress: 0,
        error_message: message,
        worker_finished_at: new Date().toISOString(),
      });

      return res.status(500).json({ error: message });
    }

    await updateJob(jobId, {
      progress: 75,
    });

    const compressedBuffer = await fs.readFile(localOutputPath);

    const finalBuffer =
      compressedBuffer.length > 0 &&
      compressedBuffer.length < originalBuffer.length
        ? compressedBuffer
        : originalBuffer;

    await updateJob(jobId, {
      progress: 90,
    });

    const { error: uploadError } = await supabase.storage
      .from(outputBucket)
      .upload(outputPath, finalBuffer, {
        contentType: "application/pdf",
        upsert: true,
      });

    if (uploadError) {
      await updateJob(jobId, {
        status: "failed",
        progress: 0,
        error_message: uploadError.message,
        worker_finished_at: new Date().toISOString(),
      });

      return res.status(500).json({ error: uploadError.message });
    }

    await updateJob(jobId, {
      status: "done",
      progress: 100,
      output_size_bytes: finalBuffer.length,
      worker_finished_at: new Date().toISOString(),
    });

    return res.json({
      ok: true,
      jobId,
      originalSizeBytes: originalBuffer.length,
      outputSizeBytes: finalBuffer.length,
      compressed: finalBuffer.length < originalBuffer.length,
    });
  } catch (error) {
    console.error(error);

    if (jobId) {
      await updateJob(jobId, {
        status: "failed",
        progress: 0,
        error_message:
          error instanceof Error ? error.message : "Unexpected worker error",
        worker_finished_at: new Date().toISOString(),
      });
    }

    return res.status(500).json({
      error: error instanceof Error ? error.message : "Unexpected worker error",
    });
  } finally {
    if (tempDir) {
      await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
    }
  }
});

const { port } = getConfig();

app.listen(port, () => {
  console.log(`PDF worker running on port ${port}`);
});