package amqp091

import "testing"

func TestConfirmSelectWaitHonorsNoWait(t *testing.T) {
	yes := confirmSelect{NoWait: true}
	if yes.wait() {
		t.Fatal("confirmSelect.wait() should be false when NoWait is set")
	}
	no := confirmSelect{NoWait: false}
	if !no.wait() {
		t.Fatal("confirmSelect.wait() should be true when NoWait is unset")
	}
}
