package bet

import (
	"fmt"
	"strconv"
	"strings"
)

type Bet struct {
	AgencyId  uint32
	FirstName string
	LastName  string
	Document  uint32
	Birthdate string
	Number    uint32
}

// ParseBet converts one raw CSV line ("first,last,document,birthdate,number")
// plus the agency id into a domain Bet.
func ParseBet(line string, agencyId uint32) (Bet, error) {
	firstName, rest, ok := strings.Cut(line, ",")
	if !ok {
		return Bet{}, fmt.Errorf("invalid bet line, expected 5 fields")
	}

	lastName, rest, ok := strings.Cut(rest, ",")
	if !ok {
		return Bet{}, fmt.Errorf("invalid bet line, expected 5 fields")
	}

	documentStr, rest, ok := strings.Cut(rest, ",")
	if !ok {
		return Bet{}, fmt.Errorf("invalid bet line, expected 5 fields")
	}

	birthdate, numberStr, ok := strings.Cut(rest, ",")
	if !ok {
		return Bet{}, fmt.Errorf("invalid bet line, expected 5 fields")
	}

	documentStr = strings.TrimSpace(documentStr)
	numberStr = strings.TrimSpace(numberStr)
	birthdate = strings.TrimSpace(birthdate)

	document, err := strconv.ParseUint(documentStr, 10, 32)
	if err != nil {
		return Bet{}, fmt.Errorf("invalid document: %w", err)
	}

	number, err := strconv.ParseUint(numberStr, 10, 32)
	if err != nil {
		return Bet{}, fmt.Errorf("invalid number: %w", err)
	}

	return Bet{
		AgencyId:  agencyId,
		FirstName: firstName,
		LastName:  lastName,
		Document:  uint32(document),
		Birthdate: birthdate,
		Number:    uint32(number),
	}, nil
}
