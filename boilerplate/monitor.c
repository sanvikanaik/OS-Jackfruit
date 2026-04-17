#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/fs.h>
#include <linux/miscdevice.h>
#include <linux/mutex.h>
#include <linux/list.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/sched.h>
#include <linux/mm.h>
#include <linux/uaccess.h>
#include <linux/pid.h>
#include <linux/signal.h>

#include "monitor_ioctl.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("OS-Jackfruit");
MODULE_DESCRIPTION("Container Memory Monitor");

/* --- Per-container entry in kernel linked list --- */
struct container_entry {
    pid_t pid;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    char container_id[MONITOR_NAME_LEN];
    int soft_warned;          // have we already warned for soft limit?
    struct list_head list;
};

/* --- Globals --- */
static LIST_HEAD(container_list);
static DEFINE_MUTEX(container_mutex);
static struct timer_list monitor_timer;

/* --- Get RSS of a process in bytes --- */
static unsigned long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    unsigned long rss = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task && task->mm)
        rss = get_mm_rss(task->mm) << PAGE_SHIFT;
    rcu_read_unlock();

    return rss;
}

/* --- Timer callback: check memory of all containers --- */
static void monitor_check(struct timer_list *t)
{
    struct container_entry *entry, *tmp;

    mutex_lock(&container_mutex);
    list_for_each_entry_safe(entry, tmp, &container_list, list) {
        struct task_struct *task;
        unsigned long rss;

        // check if process still exists
        rcu_read_lock();
        task = pid_task(find_vpid(entry->pid), PIDTYPE_PID);
        rcu_read_unlock();

        if (!task) {
            // process is gone, remove from list
            printk(KERN_INFO "monitor: container %s (pid %d) no longer exists, removing\n",
                   entry->container_id, entry->pid);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        rss = get_rss_bytes(entry->pid);

        // hard limit check
        if (rss > entry->hard_limit_bytes) {
            printk(KERN_WARNING "monitor: container %s (pid %d) exceeded HARD limit "
                   "(%lu > %lu bytes), killing!\n",
                   entry->container_id, entry->pid, rss, entry->hard_limit_bytes);
            kill_pid(find_vpid(entry->pid), SIGKILL, 1);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        // soft limit check
        if (rss > entry->soft_limit_bytes && !entry->soft_warned) {
            printk(KERN_WARNING "monitor: container %s (pid %d) exceeded SOFT limit "
                   "(%lu > %lu bytes), warning!\n",
                   entry->container_id, entry->pid, rss, entry->soft_limit_bytes);
            entry->soft_warned = 1;
        }
    }
    mutex_unlock(&container_mutex);

    // reschedule timer every 5 seconds
    mod_timer(&monitor_timer, jiffies + msecs_to_jiffies(5000));
}

/* --- ioctl handler --- */
static long monitor_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        struct container_entry *entry = kzalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry) return -ENOMEM;

        entry->pid = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warned = 0;
        strncpy(entry->container_id, req.container_id, MONITOR_NAME_LEN - 1);

        mutex_lock(&container_mutex);
        list_add(&entry->list, &container_list);
        mutex_unlock(&container_mutex);

        printk(KERN_INFO "monitor: registered container %s (pid %d) soft=%lu hard=%lu\n",
               entry->container_id, entry->pid,
               entry->soft_limit_bytes, entry->hard_limit_bytes);

    } else if (cmd == MONITOR_UNREGISTER) {
        struct container_entry *entry, *tmp;

        mutex_lock(&container_mutex);
        list_for_each_entry_safe(entry, tmp, &container_list, list) {
            if (entry->pid == req.pid) {
                printk(KERN_INFO "monitor: unregistered container %s (pid %d)\n",
                       entry->container_id, entry->pid);
                list_del(&entry->list);
                kfree(entry);
                break;
            }
        }
        mutex_unlock(&container_mutex);

    } else {
        return -EINVAL;
    }

    return 0;
}

/* --- File operations --- */
static const struct file_operations monitor_fops = {
    .owner          = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

static struct miscdevice monitor_dev = {
    .minor = MISC_DYNAMIC_MINOR,
    .name  = "container_monitor",
    .fops  = &monitor_fops,
};

/* --- Module init --- */
static int __init monitor_init(void)
{
    int ret = misc_register(&monitor_dev);
    if (ret) {
        printk(KERN_ERR "monitor: failed to register device\n");
        return ret;
    }

    // start the periodic timer
    timer_setup(&monitor_timer, monitor_check, 0);
    mod_timer(&monitor_timer, jiffies + msecs_to_jiffies(5000));

    printk(KERN_INFO "monitor: loaded, device=/dev/container_monitor\n");
    return 0;
}

/* --- Module exit --- */
static void __exit monitor_exit(void)
{
    struct container_entry *entry, *tmp;

    del_timer_sync(&monitor_timer);

    mutex_lock(&container_mutex);
    list_for_each_entry_safe(entry, tmp, &container_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }
    mutex_unlock(&container_mutex);

    misc_register(&monitor_dev);
    printk(KERN_INFO "monitor: unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);
