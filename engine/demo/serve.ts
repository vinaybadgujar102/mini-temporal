import { join } from "node:path";

const demoDir = import.meta.dir;
const port = Number(process.env.PORT) || 3456;

Bun.serve({
  port,
  async fetch(req) {
    const url = new URL(req.url);
    let pathname = url.pathname === "/" ? "/index.html" : url.pathname;
    const filePath = join(demoDir, pathname.replace(/^\//, ""));

    if (!filePath.startsWith(demoDir)) {
      return new Response("Not found", { status: 404 });
    }

    const f = Bun.file(filePath);
    if (!(await f.exists())) {
      return new Response("Not found", { status: 404 });
    }

    const ext = pathname.split(".").pop() ?? "";
    const types: Record<string, string> = {
      html: "text/html; charset=utf-8",
      css: "text/css; charset=utf-8",
      js: "text/javascript; charset=utf-8",
    };

    return new Response(f, {
      headers: { "Content-Type": types[ext] ?? "application/octet-stream" },
    });
  },
});

console.log(`mini-temporal demo → http://localhost:${port}`);
