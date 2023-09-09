package net.risingdev.springboot;

import net.risingdev.springboot.entity.WikimediaData;
import net.risingdev.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KakfaDatabaseConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KakfaDatabaseConsumer.class);

    private WikimediaDataRepository dataRepository;

    public KakfaDatabaseConsumer (WikimediaDataRepository dataRepository) {

        this.dataRepository = dataRepository;

    }


    @KafkaListener(
            topics = "wikimedia_streamchanges",
            groupId = "myGroup"
    )
    public void consume(String eventMessage) {

        LOGGER.info(String.format("Message received -> %s", eventMessage));

        WikimediaData wikimediaData = new WikimediaData();

        wikimediaData.setWikiEventData(eventMessage);

        dataRepository.save(wikimediaData);

    }

}
