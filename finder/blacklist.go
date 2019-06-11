package finder

import (
	"context"
	"regexp"
)

type BlacklistFinder struct {
	wrapped   Finder
	blacklist []*regexp.Regexp // config
	matched   bool
}

func WrapBlacklist(f Finder, blacklist []*regexp.Regexp) *BlacklistFinder {
	return &BlacklistFinder{
		wrapped:   f,
		blacklist: blacklist,
	}
}

func (p *BlacklistFinder) IsBlackListed(query string) bool {
	for i := 0; i < len(p.blacklist); i++ {
		if p.blacklist[i].MatchString(query) {
			p.matched = true
			return true
		}
	}
	return false
}

func (p *BlacklistFinder) Query(query string, from int64, until int64) (string, error) {
	if p.IsBlackListed(query) {
		return "", nil
	}

	return p.wrapped.Query(query, from, until)
}

func (p *BlacklistFinder) Execute(ctx context.Context, query string, from int64, until int64) error {
	if p.IsBlackListed(query) {
		return nil
	}

	return p.wrapped.Execute(ctx, query, from, until)
}

func (p *BlacklistFinder) List() [][]byte {
	if p.matched {
		return [][]byte{}
	}

	return p.wrapped.List()
}

// For Render
func (p *BlacklistFinder) Series() [][]byte {
	if p.matched {
		return [][]byte{}
	}

	return p.wrapped.Series()
}

func (p *BlacklistFinder) Abs(v []byte) []byte {
	return p.wrapped.Abs(v)
}
