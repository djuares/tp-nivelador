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
	fields := strings.Split(line, ",")
	if len(fields) != 5 {
		return Bet{}, fmt.Errorf("invalid bet line, expected 5 fields, got %d", len(fields))
	}

	document, err := strconv.ParseUint(strings.TrimSpace(fields[2]), 10, 32)
	if err != nil {
		return Bet{}, fmt.Errorf("invalid document: %w", err)
	}

	number, err := strconv.ParseUint(strings.TrimSpace(fields[4]), 10, 32)
	if err != nil {
		return Bet{}, fmt.Errorf("invalid number: %w", err)
	}

	return Bet{
		AgencyId:  agencyId,
		FirstName: fields[0],
		LastName:  fields[1],
		Document:  uint32(document),
		Birthdate: strings.TrimSpace(fields[3]),
		Number:    uint32(number),
	}, nil
}
