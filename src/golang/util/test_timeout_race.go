// +build race

package util

import "time"

// https://stackoverflow.com/questions/44944959/how-can-i-check-if-the-race-detector-is-enabled-at-runtime
const TestTimeout  = 100 * time.Millisecond
const SmallTimeout = 20 * time.Millisecond
const MedTimeout   = 50 * time.Millisecond
const LargeTimeout = 170 * time.Millisecond
const HugeTimeout =  1700 * time.Millisecond
const RaceDetectorOn = true

