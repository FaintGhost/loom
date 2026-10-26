package intake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/FaintGhost/loom/internal/worker"
)

func attachmentURLAllowed(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.User == nil && (u.Host == "cdn.discordapp.com" || u.Host == "media.discordapp.net")
}
func fetchAttachments(ctx context.Context, home string, cfg worker.Config, id int64, attachments []Attachment) {
	maxFiles := cfg.AttachmentMaxFiles
	maxBytes := cfg.AttachmentMaxBytes
	if maxFiles <= 0 || maxBytes <= 0 {
		return
	}
	root := filepath.Join(home, "jobs", strconv.FormatInt(id, 10))
	folder := filepath.Join(root, "attachments")
	if err := os.MkdirAll(folder, 0700); err != nil {
		return
	}
	index := map[string]string{}
	indexPath := filepath.Join(root, "attachments.json")
	if b, err := os.ReadFile(indexPath); err == nil {
		if json.Unmarshal(b, &index) != nil {
			return
		}
	}
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 || !attachmentURLAllowed(req.URL.String()) {
			return errors.New("attachment redirect left Discord CDN")
		}
		return nil
	}}
	for _, a := range attachments {
		if len(index) >= maxFiles {
			return
		}
		if index[a.URL] != "" || !attachmentURLAllowed(a.URL) || a.Size > maxBytes {
			continue
		}
		name, err := saveAttachment(ctx, client, folder, a.URL, maxBytes, cfg.AttachmentContentTypes)
		if err != nil {
			continue
		}
		index[a.URL] = name
		b, err := json.Marshal(index)
		if err != nil {
			return
		}
		temp, err := os.CreateTemp(root, ".attachments-")
		if err != nil {
			return
		}
		_, err = temp.Write(b)
		err = errors.Join(err, temp.Sync(), temp.Close())
		if err == nil {
			err = os.Rename(temp.Name(), indexPath)
		}
		if err != nil {
			_ = os.Remove(temp.Name())
			return
		}
	}
}
func saveAttachment(ctx context.Context, client *http.Client, folder, raw string, cap int64, types []string) (name string, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", raw, nil)
	if err != nil {
		return "", err
	}
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	ctype, _, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if err != nil || !slices.Contains(types, strings.ToLower(ctype)) {
		return "", errors.New("attachment content type not allowed")
	}
	if res.StatusCode != http.StatusOK || res.ContentLength > cap {
		return "", errors.New("attachment HTTP status or size rejected")
	}
	u, _ := url.Parse(raw)
	original := filepath.Base(u.Path)
	var safe strings.Builder
	for _, r := range original {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-", r) {
			safe.WriteRune(r)
		} else {
			safe.WriteByte('_')
		}
	}
	sum := sha256.Sum256([]byte(raw))
	name = hex.EncodeToString(sum[:8]) + "-" + prefix(safe.String(), 60)
	destination := filepath.Join(folder, name)
	if info, e := os.Lstat(destination); e == nil {
		if info.Mode().IsRegular() && info.Size() <= cap {
			return name, nil
		}
		return "", errors.New("unsafe existing attachment")
	}
	f, err := os.CreateTemp(folder, ".download-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	n, copyErr := io.Copy(f, io.LimitReader(res.Body, cap+1))
	err = errors.Join(copyErr, f.Sync(), f.Close())
	if err != nil {
		return "", err
	}
	if n > cap {
		return "", errors.New("attachment exceeded byte cap")
	}
	if err = os.Link(f.Name(), destination); err != nil {
		return "", err
	}
	return name, nil
}
