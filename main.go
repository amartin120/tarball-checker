package main

import (
    "archive/tar"
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "os"
    "path"
    "strings"

    "github.com/klauspost/compress/zstd"
)

type dockerManifestEntry struct {
    Config   string   `json:"Config"`
    RepoTags []string `json:"RepoTags"`
    Layers   []string `json:"Layers"`
}

type ociIndex struct {
    Manifests []struct {
        Digest string `json:"digest"`
    } `json:"manifests"`
}

func main() {
    if len(os.Args) < 2 {
        fmt.Fprintln(os.Stderr, "usage: tarcheck <tar|tar.zst>")
        os.Exit(2)
    }
    p := os.Args[1]
    f, err := os.Open(p)
    if err != nil {
        fmt.Fprintln(os.Stderr, "open:", err)
        os.Exit(2)
    }
    defer f.Close()

    // detect zstd by magic header
    var reader io.Reader = f
    var header [4]byte
    if _, err := f.Read(header[:]); err != nil {
        fmt.Fprintln(os.Stderr, "read header:", err)
        os.Exit(2)
    }
    _, _ = f.Seek(0, io.SeekStart)
    if bytes.Equal(header[:], []byte{0x28, 0xB5, 0x2F, 0xFD}) {
        dec, err := zstd.NewReader(f)
        if err != nil {
            fmt.Fprintln(os.Stderr, "zstd new reader:", err)
            os.Exit(2)
        }
        defer dec.Close()
        reader = dec
    }

    tr := tar.NewReader(reader)
    found := map[string]bool{}
    var manifestBuf, indexBuf []byte

    for {
        h, err := tr.Next()
        if err == io.EOF {
            break
        }
        if err != nil {
            fmt.Fprintln(os.Stderr, "tar read:", err)
            os.Exit(2)
        }
        // normalize names (tar can contain leading ./)
        name := strings.TrimPrefix(h.Name, "./")
        found[name] = true

        switch path.Base(name) {
        case "manifest.json":
            b, _ := io.ReadAll(tr)
            manifestBuf = b
        case "index.json":
            b, _ := io.ReadAll(tr)
            indexBuf = b
        }
    }

    if len(manifestBuf) > 0 {
        fmt.Println("Detected docker-archive (manifest.json). Checking referenced files...")
        var entries []dockerManifestEntry
        if err := json.Unmarshal(manifestBuf, &entries); err != nil {
            fmt.Fprintln(os.Stderr, "parse manifest.json:", err)
            os.Exit(2)
        }
        ok := true
        for i, e := range entries {
            fmt.Printf("entry[%d] tags=%v config=%s\n", i, e.RepoTags, e.Config)
            if !found[e.Config] {
                fmt.Printf("  MISSING: config %q\n", e.Config)
                ok = false
            }
            for _, l := range e.Layers {
                if !found[l] {
                    fmt.Printf("  MISSING: layer %q\n", l)
                    ok = false
                }
            }
        }
        if ok {
            fmt.Println("All docker-archive referenced files present.")
        }
    }

    if len(indexBuf) > 0 {
        fmt.Println("Detected OCI layout (index.json). Checking blobs/ references...")
        var idx ociIndex
        if err := json.Unmarshal(indexBuf, &idx); err != nil {
            fmt.Fprintln(os.Stderr, "parse index.json:", err)
            os.Exit(2)
        }
        ok := true
        for i, m := range idx.Manifests {
            fmt.Printf("manifest[%d] digest=%s\n", i, m.Digest)
            parts := strings.SplitN(m.Digest, ":", 2)
            if len(parts) != 2 {
                fmt.Printf("  UNEXPECTED digest format: %q\n", m.Digest)
                ok = false
                continue
            }
            algo, hex := parts[0], parts[1]
            expected := path.Join("blobs", algo, hex)
            if !found[expected] {
                fmt.Printf("  MISSING: %s\n", expected)
                ok = false
            }
        }
        if ok {
            fmt.Println("All OCI index referenced blobs present.")
        }
    }

    if len(indexBuf) == 0 && len(manifestBuf) == 0 {
        fmt.Println("No manifest.json or index.json detected; tar may have an unexpected layout.")
    }

}