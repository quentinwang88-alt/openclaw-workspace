import AppKit
import Foundation

guard CommandLine.arguments.count == 3 else {
    fputs("usage: render_text_overlay.swift <text> <output.png>\n", stderr)
    exit(2)
}

let text = CommandLine.arguments[1]
let output = URL(fileURLWithPath: CommandLine.arguments[2])
let canvas = NSSize(width: 720, height: 1280)
let image = NSImage(size: canvas)
image.lockFocus()
NSColor.clear.setFill()
NSRect(origin: .zero, size: canvas).fill(using: .copy)

let paragraph = NSMutableParagraphStyle()
paragraph.alignment = .center
paragraph.lineBreakMode = .byWordWrapping
let font = NSFont(name: "Thonburi-Bold", size: 52) ?? NSFont.systemFont(ofSize: 52, weight: .bold)
let attributes: [NSAttributedString.Key: Any] = [
    .font: font,
    .foregroundColor: NSColor.white,
    .strokeColor: NSColor.black,
    .strokeWidth: -5.0,
    .paragraphStyle: paragraph,
]
NSAttributedString(string: text, attributes: attributes).draw(
    with: NSRect(x: 40, y: 118, width: 640, height: 100),
    options: [.usesLineFragmentOrigin, .usesFontLeading]
)
image.unlockFocus()

guard let cgImage = image.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
    fputs("failed to render image\n", stderr)
    exit(1)
}
let bitmap = NSBitmapImageRep(cgImage: cgImage)
guard let data = bitmap.representation(using: .png, properties: [:]) else {
    fputs("failed to encode png\n", stderr)
    exit(1)
}
try data.write(to: output)
