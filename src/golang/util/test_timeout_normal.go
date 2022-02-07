// +build !race

package util

import "time"

// https://stackoverflow.com/questions/44944959/how-can-i-check-if-the-race-detector-is-enabled-at-runtime
const TestTimeout  = 10 * time.Millisecond
const SmallTimeout = 2 * time.Millisecond
const MedTimeout   = 5 * time.Millisecond
const LargeTimeout = 17 * time.Millisecond
const RaceDetectorOn = false


