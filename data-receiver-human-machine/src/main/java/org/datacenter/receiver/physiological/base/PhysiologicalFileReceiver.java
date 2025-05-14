package org.datacenter.receiver.physiological.base;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.receiver.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.receiver.CsvFileReceiver;

import java.io.Serial;
import java.io.Serializable;

/**
 * @author : [wangminan]
 * @description : 抽象的生理数据离线接入器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class PhysiologicalFileReceiver<T> extends CsvFileReceiver<T, PhysiologicalFileReceiverConfig> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
}
