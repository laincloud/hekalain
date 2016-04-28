package hekahelper

import "github.com/mozilla-services/heka/message"

func GetMessageStringValue(msg *message.Message, field string) (string, bool) {
	if value, ok := msg.GetFieldValue(field); ok {
		if sValue, ok := value.(string); ok {
			return sValue, ok
		}
	}
	return "", false
}

func GetMessageIntValue(msg *message.Message, field string) (int, bool) {
	if value, ok := msg.GetFieldValue(field); ok {
		if iValue, ok := value.(int64); ok {
			return int(iValue), true
		}
	}
	return 0, false
}

func GetMessageDoubleValue(msg *message.Message, field string) (float64, bool) {
	if value, ok := msg.GetFieldValue(field); ok {
		if iValue, ok := value.(float64); ok {
			return float64(iValue), true
		}
	}
	return 0, false
}
