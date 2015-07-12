package io.reactivex.lab.gateway.clients;

import rx.Observable;
import rx.functions.Func1;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

class RetryWhenNoServersAvailable implements Func1<Observable<? extends Throwable>, Observable<?>> {

    @Override
    public Observable<?> call(Observable<? extends Throwable> errStream) {
        return errStream.flatMap(err -> {
            if (err instanceof NoSuchElementException) {
                System.out.println("No hosts available, retrying after 10 seconds.");
                return Observable.timer(10, TimeUnit.SECONDS);
            }
            return Observable.error(err);
        });
    }
}
