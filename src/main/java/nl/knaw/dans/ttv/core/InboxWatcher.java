package nl.knaw.dans.ttv.core;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.ExecutorService;

public class InboxWatcher implements Runnable {

    private final Inbox inbox;
    private final ExecutorService executorService;

    public InboxWatcher(Inbox inbox, ExecutorService executorService) {
        this.inbox = inbox;
        this.executorService = executorService;
    }

    private void startWatchService() {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            inbox.getPath().register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            WatchKey key;
            while ((key = watchService.take()) != null) {
                key.pollEvents().stream()
                        .map(watchEvent -> (Path) watchEvent.context())
                        .filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(".zip"))
                        .forEach(path -> {
                            executorService.submit(inbox.createTransferItemTask(path));
                        });
                key.reset();
            }
        } catch (IOException | InterruptedException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void run() {
        startWatchService();
    }
}
