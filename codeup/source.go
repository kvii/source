// Package codeup implements source.Driver for codeup.
package codeup

import (
	"errors"
	"io"
	"io/fs"
	iurl "net/url"
	"os"
	"path"
	"strconv"
	"strings"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	devops "github.com/alibabacloud-go/devops-20210625/v4/client"
	"github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	source.Register("codeup", CodeUp{})
}

// Option is the configuration setting for the CodeUp driver.
type Option struct {
	Config  Config
	Headers map[string]*string
	Runtime *service.RuntimeOptions
}

// NewOption creates a new Option.
func NewOption(c Config) Option {
	return Option{
		Config:  c,
		Headers: make(map[string]*string),
		Runtime: new(service.RuntimeOptions),
	}
}

// Config is the configuration setting for the CodeUp driver.
type Config struct {
	ProjectId      string
	OrganizationId string
	AccessToken    string
	Path           string // repo path
	Ref            string // repo ref, default is "master".
}

func configFromUrl(url *iurl.URL) Config {
	ref := url.Fragment
	if ref == "" {
		ref = "master"
	}

	query := url.Query()
	c := Config{
		ProjectId:      query.Get("projectId"),
		OrganizationId: query.Get("organizationId"),
		AccessToken:    query.Get("accessToken"),
		Path:           url.Path,
		Ref:            ref,
	}
	return c
}

func clientConfigFromUrl(u *iurl.URL) *openapi.Config {
	key := u.User.Username()
	if key == "" {
		key = os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	}
	secret, ok := u.User.Password()
	if !ok {
		secret = os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	}

	return &openapi.Config{
		AccessKeyId:     tea.String(key),
		AccessKeySecret: tea.String(secret),
		Endpoint:        tea.String(u.Host),
	}
}

// CodeUp implements source.Driver for CodeUp.
type CodeUp struct {
	option     Option
	client     *devops.Client
	migrations *source.Migrations
}

// WithInstance returns a new CodeUp driver instance configured with parameters
func WithInstance(client *devops.Client, option Option) (source.Driver, error) {
	gn := &CodeUp{
		option:     option,
		client:     client,
		migrations: source.NewMigrations(),
	}

	err := gn.readDirectory()
	if err != nil {
		return nil, err
	}
	return gn, nil
}

// Open returns a new driver instance configured with parameters
// coming from the URL string. Migrate will call this function
// only once per instance.
func (s CodeUp) Open(url string) (source.Driver, error) {
	u, err := iurl.Parse(url)
	if err != nil {
		return nil, err
	}

	client, err := devops.NewClient(clientConfigFromUrl(u))
	if err != nil {
		return nil, err
	}

	cn := CodeUp{
		client:     client,
		migrations: source.NewMigrations(),
		option:     NewOption(configFromUrl(u)),
	}

	err = cn.readDirectory()
	if err != nil {
		return nil, err
	}
	return cn, nil
}

// Close closes the underlying source instance managed by the driver.
func (s CodeUp) Close() error { return nil }

// First returns the very first migration version available to the driver.
func (s CodeUp) First() (version uint, err error) {
	v, ok := s.migrations.First()
	if ok {
		return v, nil
	}

	return 0, &fs.PathError{
		Op:   "first",
		Path: s.option.Config.Path,
		Err:  fs.ErrNotExist,
	}
}

// Prev returns the previous version for a given version available to the driver.
func (s CodeUp) Prev(version uint) (prevVersion uint, err error) {
	v, ok := s.migrations.Prev(version)
	if ok {
		return v, nil
	}

	return 0, &fs.PathError{
		Op:   "prev for version " + strconv.FormatUint(uint64(version), 10),
		Path: s.option.Config.Path,
		Err:  fs.ErrNotExist,
	}
}

// Next returns the next version for a given version available to the driver.
func (s CodeUp) Next(version uint) (nextVersion uint, err error) {
	v, ok := s.migrations.Next(version)
	if ok {
		return v, nil
	}

	return 0, &fs.PathError{
		Op:   "next for version " + strconv.FormatUint(uint64(version), 10),
		Path: s.option.Config.Path,
		Err:  fs.ErrNotExist,
	}
}

// ReadUp returns the UP migration body and an identifier that helps
// finding this migration in the source for a given version.
func (s CodeUp) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	m, ok := s.migrations.Up(version)
	if !ok {
		return nil, "", &fs.PathError{
			Op:   "read version " + strconv.FormatUint(uint64(version), 10),
			Path: s.option.Config.Path,
			Err:  fs.ErrNotExist,
		}
	}

	content, err := s.read(m.Raw)
	if err != nil {
		return nil, "", err
	}

	sr := strings.NewReader(content)
	return io.NopCloser(sr), m.Identifier, nil
}

// ReadDown returns the DOWN migration body and an identifier that helps
// finding this migration in the source for a given version.
func (s CodeUp) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	m, ok := s.migrations.Down(version)
	if !ok {
		return nil, "", &fs.PathError{
			Op:   "read version " + strconv.FormatUint(uint64(version), 10),
			Path: s.option.Config.Path,
			Err:  fs.ErrNotExist,
		}
	}

	content, err := s.read(m.Raw)
	if err != nil {
		return nil, "", err
	}

	sr := strings.NewReader(content)
	return io.NopCloser(sr), m.Identifier, nil
}

func (s CodeUp) readDirectory() error {
	resp, err := s.client.ListRepositoryTreeWithOptions(
		tea.String(s.option.Config.ProjectId),
		&devops.ListRepositoryTreeRequest{
			OrganizationId: tea.String(s.option.Config.OrganizationId),
			AccessToken:    tea.String(s.option.Config.AccessToken),
			Path:           tea.String(s.option.Config.Path),
		},
		s.option.Headers,
		s.option.Runtime,
	)
	if err != nil {
		return err
	}
	body := resp.Body
	if !tea.BoolValue(body.Success) {
		return errors.New(tea.StringValue(body.ErrorMessage))
	}

	for _, v := range body.Result {
		m, err := source.Parse(tea.StringValue(v.Name))
		if err != nil {
			return err
		}
		s.migrations.Append(m)
	}
	return nil
}

// read content of file.
//
// Because there is no way to get the http body of file content,
// so read will return content directly (instead of return a body reader).
func (s CodeUp) read(filePath string) (string, error) {
	resp, err := s.client.GetFileBlobsWithOptions(
		tea.String(s.option.Config.ProjectId),
		&devops.GetFileBlobsRequest{
			OrganizationId: tea.String(s.option.Config.OrganizationId),
			AccessToken:    tea.String(s.option.Config.AccessToken),
			FilePath:       tea.String(path.Join(s.option.Config.Path, filePath)),
			Ref:            tea.String(s.option.Config.Ref),
		},
		s.option.Headers,
		s.option.Runtime,
	)
	if err != nil {
		return "", err
	}
	body := resp.Body
	if !tea.BoolValue(body.Success) {
		return "", errors.New(tea.StringValue(body.ErrorMessage))
	}
	return tea.StringValue(body.Result.Content), nil
}
