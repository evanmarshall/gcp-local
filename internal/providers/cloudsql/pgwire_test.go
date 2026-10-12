package cloudsql

import "testing"

// TestScramVector checks the SCRAM-SHA-256 client against RFC 7677 section 3.
func TestScramVector(t *testing.T) {
	scram := newScramClient("pencil")
	scram.nonce = "rOprNGfwEbeRWgbNEkqO"
	scram.clientFirst()
	scram.firstBare = "n=user,r=rOprNGfwEbeRWgbNEkqO"
	final, err := scram.clientFinal("r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
	must(t, err)
	if final != "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=" {
		t.Fatalf("client final = %q", final)
	}
	if !scram.verify("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=") || scram.verify("v=AAAA") {
		t.Fatal("server signature verification is wrong")
	}
	if _, err := scram.clientFinal("r=other,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"); err == nil {
		t.Fatal("accepted a server nonce without the client prefix")
	}
}

func TestQuoting(t *testing.T) {
	if quoteIdentifier(`a"b`) != `"a""b"` || quoteLiteral(`a'b\c`) != `E'a''b\\c'` {
		t.Fatal("quoting is wrong")
	}
	row, err := parseRow([]byte{0, 2, 0, 0, 0, 1, 'x', 0xff, 0xff, 0xff, 0xff})
	if err != nil || len(row) != 2 || row[0] != "x" || row[1] != "" {
		t.Fatalf("row = %q, %v", row, err)
	}
	if _, err := parseRow([]byte{0, 1, 0, 0, 0, 9}); err == nil {
		t.Fatal("accepted a truncated row")
	}
}
