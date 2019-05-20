package com.skuniv.cs.geonyeong.kaggle.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum KafkaTopicType {
    QUESTION_RECENT,
    QUESTION_DELETE,
    ANSWER_RECENT,
    ANSWER_DELETE,
    ACCOUNT_RECENT,
    ACCOUNT_DELETE
}
