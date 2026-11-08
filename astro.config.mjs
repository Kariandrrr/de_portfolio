import { defineConfig } from 'astro/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const BASE = '/de_portfolio/';

const autoBase = () => ({
    name: 'auto-base',
    hooks: {
        'astro:build:done': ({ dir }) => {
            const outDir = fileURLToPath(dir);

            const walk = (folder) =>
                fs.readdirSync(folder, { withFileTypes: true }).flatMap((item) => {
                    const full = path.join(folder, item.name);
                    return item.isDirectory() ? walk(full) : [full];
                });

            for (const file of walk(outDir)) {
                if (!/\.(html|css|js|json)$/.test(file)) continue;

                let content = fs.readFileSync(file, 'utf8');
                const original = content;

                content = content
                    // <a href="/...">, <img src="/...">
                    .replace(/(href|src|srcset)=(["'])\/(?!\/|de_portfolio\/)/g, `$1=$2${BASE}`)
                    // url(/...) в CSS
                    .replace(/url\((["']?)\/(?!\/|de_portfolio\/)/g, `url($1${BASE}`);

                if (content !== original) fs.writeFileSync(file, content);
            }
        },
    },
});

export default defineConfig({
    site: 'https://kariandrrr.github.io',
    base: BASE,
    integrations: [autoBase()],
});