package com.systemdesign.orderpipeline.service;

import com.systemdesign.orderpipeline.model.OrderItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service managing inventory stock counts.
 */
@Service
public class InventoryStockService {

    private static final Logger log = LoggerFactory.getLogger(InventoryStockService.class);

    private final Map<String, Integer> stockStore = new ConcurrentHashMap<>();

    public InventoryStockService() {
        stockStore.put("prod_apple_iphone", 100);
        stockStore.put("prod_macbook_pro", 50);
        stockStore.put("prod_sony_headphones", 200);
    }

    public synchronized boolean reserveStock(List<OrderItem> items) {
        for (OrderItem item : items) {
            int currentStock = stockStore.getOrDefault(item.getProductId(), 0);
            if (currentStock < item.getQuantity()) {
                log.warn("Insufficient stock for productId={}: available={}, requested={}",
                        item.getProductId(), currentStock, item.getQuantity());
                return false;
            }
        }

        for (OrderItem item : items) {
            stockStore.compute(item.getProductId(), (id, current) -> (current == null ? 0 : current) - item.getQuantity());
            log.info("Deducted stock for productId={}. Remaining={}", item.getProductId(), stockStore.get(item.getProductId()));
        }

        return true;
    }

    public int getStock(String productId) {
        return stockStore.getOrDefault(productId, 0);
    }
}
