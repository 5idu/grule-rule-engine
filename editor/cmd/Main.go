package main

import (
	"github.com/5idu/grule-rule-engine/editor"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.TraceLevel)
	editor.Start()
}
