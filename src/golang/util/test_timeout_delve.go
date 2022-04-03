// +build !race
// +build delve

package util

import "time"

// https://stackoverflow.com/questions/44944959/how-can-i-check-if-the-race-detector-is-enabled-at-runtime
const TestTimeout  = 10 *  time.Hour
const SmallTimeout = 10 *  time.Hour
const MedTimeout   = 10 *  time.Hour
const LargeTimeout = 10 *  time.Hour
const HugeTimeout  = 10 *  time.Hour
const RaceDetectorOn = false


