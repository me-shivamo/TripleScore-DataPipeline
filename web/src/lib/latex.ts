import katex from "katex";

export function renderLatex(text: string): string {
  // Replace display math $$...$$
  let result = text.replace(/\$\$([\s\S]*?)\$\$/g, (_, math) => {
    try {
      return katex.renderToString(math.trim(), { displayMode: true, throwOnError: false });
    } catch {
      return `<span class="text-red-400">${math}</span>`;
    }
  });

  // Replace inline math $...$
  result = result.replace(/\$([^$\n]+?)\$/g, (_, math) => {
    try {
      return katex.renderToString(math.trim(), { displayMode: false, throwOnError: false });
    } catch {
      return `<span class="text-red-400">${math}</span>`;
    }
  });

  // Replace image references: "Image: description" with nothing (they're text descriptions of images)
  // and actual Cloudinary URLs
  result = result.replace(
    /(https?:\/\/res\.cloudinary\.com\/[^\s)]+)/g,
    '<img src="$1" class="my-2 max-w-full rounded" />'
  );

  // Convert newlines to <br>
  result = result.replace(/\n/g, "<br />");

  return result;
}
