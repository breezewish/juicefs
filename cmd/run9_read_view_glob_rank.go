package cmd

import (
	"math"
	"path"
	"strings"
	"unicode"
)

const (
	run9ReadViewGlobPathIdentityScore = 1 << 18
	run9ReadViewGlobLabelPrefixScore  = 1 << 17
	run9ReadViewGlobLabelScore        = 1 << 16
)

type run9ReadViewGlobPathScore struct {
	score      int
	labelMatch bool
}

// compareRun9ReadViewGlobPaths follows VS Code File Quick Open's label,
// description, and path priorities. Candidate matching remains the Glob's
// responsibility, so this scorer only needs contiguous matches.
func compareRun9ReadViewGlobPaths(left string, right string, query string) int {
	if query == "" {
		return strings.Compare(left, right)
	}
	leftScore := scoreRun9ReadViewGlobPath(left, query)
	rightScore := scoreRun9ReadViewGlobPath(right, query)
	if leftScore.score == run9ReadViewGlobPathIdentityScore || rightScore.score == run9ReadViewGlobPathIdentityScore {
		if leftScore.score != rightScore.score {
			return compareRun9ReadViewGlobScore(leftScore.score, rightScore.score)
		}
	}
	if leftScore.score > run9ReadViewGlobLabelScore || rightScore.score > run9ReadViewGlobLabelScore {
		if leftScore.score != rightScore.score {
			return compareRun9ReadViewGlobScore(leftScore.score, rightScore.score)
		}
		leftLabel, rightLabel := path.Base(left), path.Base(right)
		if len([]rune(leftLabel)) != len([]rune(rightLabel)) {
			return len([]rune(leftLabel)) - len([]rune(rightLabel))
		}
	}
	if leftScore.score != rightScore.score {
		return compareRun9ReadViewGlobScore(leftScore.score, rightScore.score)
	}
	if leftScore.labelMatch != rightScore.labelMatch {
		if leftScore.labelMatch {
			return -1
		}
		return 1
	}
	return compareRun9ReadViewGlobFallback(left, right, query)
}

func scoreRun9ReadViewGlobPath(filePath string, query string) run9ReadViewGlobPathScore {
	if filePath == query {
		return run9ReadViewGlobPathScore{score: run9ReadViewGlobPathIdentityScore, labelMatch: true}
	}
	label := path.Base(filePath)
	description := path.Dir(filePath)
	if description == "." {
		description = ""
	}
	if !strings.ContainsAny(query, "/\\") || description == "" {
		if score, _, _ := scoreRun9ReadViewGlobText(label, query); score > 0 {
			baseScore := run9ReadViewGlobLabelScore
			if strings.HasPrefix(strings.ToLower(label), strings.ToLower(query)) {
				baseScore = run9ReadViewGlobLabelPrefixScore + int(math.Round(float64(len([]rune(query)))*100/float64(len([]rune(label)))))
			}
			return run9ReadViewGlobPathScore{score: baseScore + score, labelMatch: true}
		}
	}
	normalizedQuery := strings.ReplaceAll(query, "\\", "/")
	if score, _, end := scoreRun9ReadViewGlobText(filePath, normalizedQuery); score > 0 {
		labelStart := len([]rune(filePath)) - len([]rune(label))
		return run9ReadViewGlobPathScore{
			score:      score,
			labelMatch: end > labelStart,
		}
	}
	return run9ReadViewGlobPathScore{}
}

func scoreRun9ReadViewGlobText(target string, query string) (bestScore int, bestStart int, bestEnd int) {
	targetRunes := []rune(target)
	queryRunes := []rune(query)
	for start := 0; len(queryRunes) > 0 && start+len(queryRunes) <= len(targetRunes); start++ {
		score := 0
		for offset, queryRune := range queryRunes {
			targetIndex := start + offset
			targetRune := targetRunes[targetIndex]
			if unicode.ToLower(queryRune) != unicode.ToLower(targetRune) {
				score = 0
				break
			}
			score++
			if queryRune == targetRune {
				score++
			}
			if offset > 0 {
				score += min(offset, 3)*6 + max(0, offset-3)*3
			}
			if targetIndex == 0 {
				score += 8
			} else if bonus := run9ReadViewGlobSeparatorScore(targetRunes[targetIndex-1]); bonus != 0 {
				score += bonus
			} else if offset == 0 && unicode.IsUpper(targetRune) {
				score += 2
			}
		}
		if score >= bestScore && score > 0 {
			bestScore = score
			bestStart = start
			bestEnd = start + len(queryRunes)
		}
	}
	return bestScore, bestStart, bestEnd
}

func run9ReadViewGlobSeparatorScore(value rune) int {
	switch value {
	case '/', '\\':
		return 5
	case '_', '-', '.', ' ', '\'', '"', ':':
		return 4
	default:
		return 0
	}
}

func compareRun9ReadViewGlobFallback(left string, right string, query string) int {
	leftLabel, rightLabel := path.Base(left), path.Base(right)
	leftDescription, rightDescription := path.Dir(left), path.Dir(right)
	if leftDescription == "." {
		leftDescription = ""
	}
	if rightDescription == "." {
		rightDescription = ""
	}
	leftLength := len([]rune(leftLabel)) + len([]rune(leftDescription))
	rightLength := len([]rune(rightLabel)) + len([]rune(rightDescription))
	if leftLength != rightLength {
		return leftLength - rightLength
	}
	if len([]rune(left)) != len([]rune(right)) {
		return len([]rune(left)) - len([]rune(right))
	}
	if compared := compareRun9ReadViewGlobText(leftLabel, rightLabel, query); compared != 0 {
		return compared
	}
	if compared := compareRun9ReadViewGlobText(leftDescription, rightDescription, query); compared != 0 {
		return compared
	}
	if compared := compareRun9ReadViewGlobText(left, right, query); compared != 0 {
		return compared
	}
	return strings.Compare(left, right)
}

func compareRun9ReadViewGlobText(left string, right string, query string) int {
	leftLower, rightLower, queryLower := strings.ToLower(left), strings.ToLower(right), strings.ToLower(query)
	leftPrefix, rightPrefix := strings.HasPrefix(leftLower, queryLower), strings.HasPrefix(rightLower, queryLower)
	if leftPrefix != rightPrefix {
		if leftPrefix {
			return -1
		}
		return 1
	}
	if leftPrefix && len([]rune(leftLower)) != len([]rune(rightLower)) {
		return len([]rune(leftLower)) - len([]rune(rightLower))
	}
	leftSuffix, rightSuffix := strings.HasSuffix(leftLower, queryLower), strings.HasSuffix(rightLower, queryLower)
	if leftSuffix != rightSuffix {
		if leftSuffix {
			return -1
		}
		return 1
	}
	return strings.Compare(leftLower, rightLower)
}

func compareRun9ReadViewGlobScore(left int, right int) int {
	if left > right {
		return -1
	}
	return 1
}
